const mysql = require('mysql2/promise');
const axios = require('axios');
require('dotenv').config();


// 日志系统
const logger = {
  info: (...args) => console.log(`[${new Date().toISOString()}] INFO:`, ...args),
  error: (...args) => console.error(`[${new Date().toISOString()}] ERROR:`, ...args),
  debug: (...args) => process.env.DEBUG && console.log(`[${new Date().toISOString()}] DEBUG:`, ...args)
};

// 数据库连接池
const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  connectTimeout: 10000,
  decimalNumbers: true,
  supportBigNumbers: true
});

class FilecoinSyncer {
  constructor(addresses = []) {
    this.apiBase = process.env.FILFOX_API || 'https://filfox.info/api/v1';
    this.addresses = this.normalizeAddresses(addresses);
    this.pageSize = 100;
  }

  normalizeAddresses(addresses) {
    return addresses.map(addr => {
      return addr.startsWith('f0') ? addr : `f0${addr.slice(1)}`;
    });
  }

  async initialize() {
    await this.verifyTables();
  }

  async verifyTables() {
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fil_transfers (
          cid VARCHAR(512) PRIMARY KEY,
          from_addr VARCHAR(255) NOT NULL,
          to_addr VARCHAR(255) NOT NULL,
          value DECIMAL(36,18) UNSIGNED NOT NULL,
          height INT UNSIGNED NOT NULL,
          direction ENUM('in','out') NOT NULL,
          timestamp DATETIME(3) NOT NULL,
          type VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
      `);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fil_last_count (
          address VARCHAR(512) PRIMARY KEY,
          last_count int NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
      `);
    } catch (err) {
      logger.error('表创建失败:', err);
      throw err;
    }
  }

  async fullSync(address) {
    try {
      logger.info(`启动全量同步地址：${address}`);
      let currentPage = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const transfers = await this.getTransfers(address, currentPage, this.pageSize);
        if (transfers.length === 0) {
          hasMoreData = false;
          break;
        }

        await this.saveTransfers(address, transfers);
        currentPage++;
      }

      logger.info(`全量同步完成，地址: ${address}`);
    } catch (err) {
      logger.error(`同步地址 ${address} 时出错:`, err);
    }
  }

  async addSync(address, localCount, totalCount){
    try {
      logger.info(`启动增量同步地址：${address}`);
      // 需要同步的记录数量
      const needSyncCount = totalCount - localCount;
      // 如果没有需要同步的记录，直接返回
      if (needSyncCount <= 0) {
        return;
      }

      const totalPages = Math.ceil(needSyncCount / this.pageSize);
      let syncedCount = 0;
      for (let page = 0; page < totalPages; page++) {
        // 计算当前页的记录数
        const currentPageSize = Math.min(this.pageSize, needSyncCount - syncedCount);
        // 获取当前页的记录
        const transfers = await this.getTransfers(address, page, currentPageSize);
        // 确保API返回的记录数与预期一致
        if (transfers.length === 0) {
          logger.warn(`地址 ${address} 第 ${page + 1} 页无数据`);
          break;
        }
        // 保存当前页的数据
        await this.saveTransfers(address, transfers);
        // 累计已同步的记录数
        syncedCount += transfers.length;
        logger.info(`地址 ${address} 已同步 ${syncedCount} 条记录，共需同步 ${needSyncCount} 条`);
      }
      // 更新本地记录总数
      await this.updateTotalCount(address, totalCount);
      logger.info(`地址 ${address} 增量同步完成`);
    } catch (err) {
      logger.error(`同步地址 ${address} 时出错:`, err);
    }
  }

  async getTransfers(address, page, pageSize) {
    const url = new URL(`${this.apiBase}/address/${address}/transfers`);
    url.searchParams.append('pageSize', pageSize);
    url.searchParams.append('page', page);

    try {
      const response = await axios.get(url.toString(), {
        timeout: 20000,
        headers: { 'User-Agent': 'FilecoinSyncer/1.0' }
      });
      if (!response.data || !response.data.transfers) {
        logger.error(`API返回数据格式不正确: ${JSON.stringify(response.data)}`);
        return [];
      }

      logger.debug(`获取到 ${response.data.transfers.length} 条记录，地址: ${address}, 页码: ${page}`);
      return response.data.transfers || [];

    } catch (err) {
      logger.error(`获取转账记录失败: ${err.message}`);
      return [];
    }
  }

  async getTotalCount(address){
    const url = new URL(`${this.apiBase}/address/${address}/transfers`);
    try {
      const response = await axios.get(url.toString(), {
        timeout: 20000,
        headers: { 'User-Agent': 'FilecoinSyncer/1.0' }
      });
      if (!response.data || !response.data.totalCount) {
        logger.error(`API返回数据格式不正确: ${JSON.stringify(response.data)}`);
        return 0;
      }
      logger.debug(`获取到总记录数: ${response.data.totalCount}, 地址: ${address}`);
      return response.data.totalCount || 0;
    } catch (err) {
      logger.error(`获取总记录失败: ${err.message}`);
      return [];
    }
  }

  generateUniqueId() {
    if (!this.uniqueIdCounter) {
      this.uniqueIdCounter = 0;
    }
    return this.uniqueIdCounter++;
  }

  async saveTransfers(address, transfers) {
    if (transfers.length === 0) return;

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      const values = transfers.map(t => ([
        `unknown_${t.message}_${t.height}_${t.timestamp}_${t.type}_${Date.now()}_${this.generateUniqueId()}`,
        t.from,
        t.to,
        this.attoToFil(t.value),
        t.height,
        t.from === address ? 'out' : 'in',
        new Date(t.timestamp * 1000),
        t.type || 'unknown'
      ]));

      await conn.query(
        `INSERT INTO fil_transfers 
         (cid, from_addr, to_addr, value, height, direction, timestamp, type) 
         VALUES ?`,
        [values]
      );

      await conn.commit();
      logger.info(`成功写入 ${values.length} 条记录`);
    } catch (err) {
      await conn.rollback();
      logger.error(`保存转账记录失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }
  

  async saveTotalCount(address){
    const conn = await pool.getConnection();
    const totalCount = await this.getTotalCount(address);
    try {
      await conn.beginTransaction();
      await conn.query(
        `INSERT INTO fil_last_count (address, last_count) VALUES (?, ?)`,
        [address, totalCount]
      );

      await conn.commit();
      logger.info(`成功写入`);
      
    } catch (err) {
      await conn.rollback();
      logger.error(`保存失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  async selectTotalCount(address){
    const conn = await pool.getConnection();
    try {
      const result = await conn.query(
        `select last_count from fil_last_count where address = ?`,
        [address]
      );
      return result[0][0].last_count;
    } catch (err) {
      logger.error(`读取失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  async updateTotalCount(address, totalCount){
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await conn.query(
        `UPDATE fil_last_count set last_count = ? where address = ?`,
        [totalCount, address]
      );

      await conn.commit();
      logger.info(`成功修改`);
    } catch (err) {
      await conn.rollback();
      logger.error(`修改失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  attoToFil(atto) {
    try {
      const bigVal = BigInt(atto.toString().replace(/\D/g, ''));
      return Number(bigVal / 10n**18n) + Number(bigVal % 10n**18n) / 1e18;
    } catch {
      return 0;
    }
  }

  //获取有效算力
  async getPow(address){
    const url = new URL(`${this.apiBase}/address/${address}/power-stats`);
    try {
      const response = await axios.get(url.toString(), {
        timeout: 20000,
        headers: { 'User-Agent': 'FilecoinSyncer/1.0' }
      });
      let res = response.data;
      return res[res.length - 1] || [];
    } catch (err) {
      logger.error(`获取记录失败: ${err.message}`);
      return [];
    }
  }

  //获取矿工信息
  async getTotalPow(address){
    const url = new URL(`${this.apiBase}/address/${address}`);
    try {
      const response = await axios.get(url.toString(), {
        timeout: 20000,
        headers: { 'User-Agent': 'FilecoinSyncer/1.0' }
      });
      return response.data || 0;
    } catch (err) {
      logger.error(`获取记录失败: ${err.message}`);
      return [];
    }
  }

  //获取账户信息
  async getBal(address){
    const url = new URL(`${this.apiBase}/address/${address}/balance-stats`);
    try {
      const response = await axios.get(url.toString(), {
        timeout: 20000,
        headers: { 'User-Agent': 'FilecoinSyncer/1.0' }
      });
      let res = response.data;
      return res[res.length - 1] || [];
    } catch (err) {
      logger.error(`获取记录失败: ${err.message}`);
      return [];
    }
  }

  //获取总收入信息
  async selectTransRecieve(address){
    const conn = await pool.getConnection();
    try {
      const result = await conn.query(
        `select sum(value) as total from fil_transfers where type = 'receive' and to_addr = ?`,
        [address]
      );
      return result[0][0].total;
    } catch (err) {
      logger.error(`读取失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  //获取转出信息
  async selectTransSend(address){
    const conn = await pool.getConnection();
    try {
      const result = await conn.query(
        `select sum(value) as total from fil_transfers where type = 'send' and from_addr = ?`,
        [address]
      );
      return result[0][0].total || 0;
    } catch (err) {
      logger.error(`读取失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  //获取总产出
  async selectTransProduce(address){
    const conn = await pool.getConnection();
    try {
      const result = await conn.query(
        `select sum(value) as total from fil_transfers where type = 'reward' and to_addr = ?`,
        [address]
      );
      return result[0][0].total || 0;
    } catch (err) {
      logger.error(`读取失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  //获取总销毁
  async selectTransBurn(address){
    const conn = await pool.getConnection();
    try {
      const result = await conn.query(
        `select sum(value) as total from fil_transfers where type = 'burn' and from_addr = ?`,
        [address]
      );
      return result[0][0].total || 0;
    } catch (err) {
      logger.error(`读取失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }

  //获取某天的收益
  async selectTransProduceByDate(address, date){
    const conn = await pool.getConnection();
    try {
      const result = await conn.query(
        `select sum(value) as total from fil_transfers where type = 'reward' and to_addr = ? and DATE(timestamp) = ? ` ,
        [address, date]
      );
      return result[0][0].total || 0;
    } catch (err) {
      logger.error(`读取失败: ${err.message}`);
    } finally {
      conn.release();
    }
  }
}

//全量同步
async function all(){
  try {
    const addresses = process.env.ADDRESSES?.split(',').filter(Boolean) || [];
    if (addresses.length === 0) {
      throw new Error('必须在.env文件中配置ADDRESSES环境变量');
    }

    const syncer = new FilecoinSyncer(addresses);
    await syncer.initialize();

    for (const address of addresses) {
      await syncer.saveTotalCount(address);
      await syncer.fullSync(address);

    }
  } catch (err) {
    logger.error(`致命错误: ${err.message}`);
    process.exit(1);
  }
}

//增量同步
async function add(){
  try {
    const addresses = process.env.ADDRESSES?.split(',').filter(Boolean) || [];
    if (addresses.length === 0) {
      throw new Error('必须在.env文件中配置ADDRESSES环境变量');
    }

    const syncer = new FilecoinSyncer(addresses);
    await syncer.initialize();

    for (const address of addresses) {
      const local = await syncer.selectTotalCount(address);
      const net = await syncer.getTotalCount(address);
      console.log("地址:", address, "本地条数:", local, "链上:", net, "相差:", net - local);

      await syncer.addSync(address, local, net);

    }
  } catch (err) {
    logger.error(`致命错误: ${err.message}`);
    process.exit(1);
  }
}

//输出报告
async function report(){
  try {
    const addresses = process.env.ADDRESSES?.split(',').filter(Boolean) || [];
    if (addresses.length === 0) {
      throw new Error('必须在.env文件中配置ADDRESSES环境变量');
    }
    const syncer = new FilecoinSyncer(addresses);
    await syncer.initialize();

    //总信息
    let allZsl = 0, allYxsl = 0, allSqzy = 0, allKyye = 0, allReceive = 0, allSend = 0, 
    allProduce = 0, allJlscsy = 0, allBurn = 0, allCanUse = 0, allTodayProduce = 0, allYesterdayProduce = 0;

    for (const address of addresses) {
      let totalPower = await syncer.getTotalPow(address);
        let zsl = totalPower.miner.sectors.live;
      let power = await syncer.getPow(address);
        let yxsl = power.qualityAdjPower;
      let balance = await syncer.getBal(address);
        let sqzy = balance.sectorPledgeBalance;
        let kyye = balance.availableBalance;
        let jlscsy = balance.vestingFunds;

      let receive = await syncer.selectTransRecieve(address);
      let send = await syncer.selectTransSend(address);
      let produce = await syncer.selectTransProduce(address);
      let burn = await syncer.selectTransBurn(address);

      let date= new Date();
      console.log(date.toLocaleString());
      let today = date.toISOString().split('T')[0];
      date.setDate(date.getDate() - 1);
      let yesterday = date.toISOString().split('T')[0];
      let todayProduce = await syncer.selectTransProduceByDate(address, today);
      let yesterdayProduce = await syncer.selectTransProduceByDate(address, yesterday);
      let canUse = (produce - syncer.attoToFil(jlscsy) - burn);

      console.log(
        "地址：", address, '\n', 
        "总算力：", (zsl / 32 / 102.4).toFixed(3), '\n', 
        "有效算力：", (yxsl / 1024**5).toFixed(3), '\n', 
        "扇区质押：", syncer.attoToFil(sqzy).toFixed(3),'\n', 
        "可用余额：", syncer.attoToFil(kyye).toFixed(3),'\n', 
        "总转入：", receive.toFixed(3),'\n', 
        "总转出：", send.toFixed(3),'\n', 
        "总收益：", produce.toFixed(3),'\n', 
        "奖励锁仓收益：", syncer.attoToFil(jlscsy).toFixed(3),'\n', 
        "总销毁：", burn.toFixed(3),'\n', 
        "可使用收益：", canUse.toFixed(3), "质押方：", (canUse * 0.55).toFixed(3), "我方：", (canUse * 0.45).toFixed(3), '\n', 
        yesterday,"产出：", yesterdayProduce.toFixed(3),'\n', 
        today,"产出：", todayProduce.toFixed(3),'\n', 
      );
      //总信息 allZsl, allYxsl, allSqzy, allKyye, allReceive, allSend, allProduce, allJlscsy, allBurn, allCanUse, allTodayProduce, allYesterdayProduce
      allZsl += (zsl / 32 / 102.4);
      allYxsl += (yxsl / 1024**5);
      allSqzy += syncer.attoToFil(sqzy);
      allKyye += syncer.attoToFil(kyye);
      allReceive += receive;
      allSend += send;
      allProduce += produce;
      allJlscsy += syncer.attoToFil(jlscsy);
      allBurn += burn;
      allCanUse += canUse;
      allTodayProduce += todayProduce;
      allYesterdayProduce += yesterdayProduce;
      
    }
    
    console.log(
      "汇总：", '\n', 
      "总算力：", allZsl.toFixed(3), '\n', 
      "有效算力：", allYxsl.toFixed(3), '\n', 
      "扇区质押：", allSqzy.toFixed(3),'\n', 
      "可用余额：", allKyye.toFixed(3),'\n', 
      "总转入：", allReceive.toFixed(3),'\n', 
      "总转出：", allSend.toFixed(3),'\n', 
      "总收益：", allProduce.toFixed(3),'\n', 
      "奖励锁仓收益：", allJlscsy.toFixed(3),'\n', 
      "总销毁：", allBurn.toFixed(3),'\n', 
      "可使用收益：", allCanUse.toFixed(3), "质押方：", (allCanUse * 0.55).toFixed(3), "我方：", (allCanUse * 0.45).toFixed(3), '\n', 
      "昨日产出：", allYesterdayProduce.toFixed(3),'\n', 
      "今日产出：", allTodayProduce.toFixed(3),'\n', 
    );

  } catch (err) {
    logger.error(`致命错误: ${err.message}`);
    process.exit(1);
  }
}


async function main() {
  //第一次请执行
  //await all();
  await add();
  report();
}

if (require.main === module) {
  main();
}
