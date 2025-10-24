import re
import os.path
import asyncio
import aiomysql

from typing import Optional
from astrbot.api import logger
from astrbot.core import AstrBotConfig
from astrbot.api.star import Context, Star, register
from astrbot.api.event import filter, AstrMessageEvent

@register("群星回响", "SXHLY", "一个私有工具但不限制使用", "v1.1.0")
class StarsEchoes(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.pool = None
        self.main_loop = asyncio.get_event_loop()
        self.conn: Optional[aiomysql.Connection] = None

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        logger.info("开始初始化")
        logger.info("初始化第一步")
        await self.mysql_connection()
        logger.info("初始化第二步")
        await self.init_database()
        logger.info("初始化完成")

    async def mysql_connection(self):
        try:
            self.pool = await aiomysql.create_pool(  # type: ignore
                host=self.config.get("host", "127.0.0.1"),
                port=self.config.get("port", 3306),
                user=self.config.get("user"),
                password=self.config.get("password"),
                db=self.config.get("database"),
                autocommit=self.config.get("autocommit"),
                minsize=self.config.get("minsize", 1),
                maxsize=self.config.get("maxsize", 100),
            )
        except Exception as e:
            logger.error(f"mysql数据库初始化失败: {str(e)}", exc_info=True)
            logger.info("尝试直接使用已配置的sql文件")
            await self.init_database()
            raise

    async def init_database(self):
        """读取指定文件夹下的所有SQL文件并执行"""
        sql_folder = self.config.get("sql_folder")
        if not sql_folder:
            logger.info("未配置sql_folder，跳过数据库初始化")
            return

        if not os.path.exists(sql_folder):
            logger.warning(f"SQL文件夹不存在: {sql_folder}")
            return

        # 获取所有.sql文件并按文件名排序
        sql_files = sorted([f for f in os.listdir(sql_folder) if f.endswith('.sql')])
        if not sql_files:
            logger.info(f"在文件夹 {sql_folder} 中未找到SQL文件")
            return

        logger.info(f"找到 {len(sql_files)} 个SQL文件需要执行")

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # cursor.execute("FLUSH TABLES")
                for sql_file in sql_files:
                    file_path = os.path.join(sql_folder, sql_file)
                    logger.info(f"执行SQL文件: {sql_file}")

                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            sql_content = f.read()

                        # 按分号分割SQL语句并执行
                        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

                        for i, stmt in enumerate(statements):
                            if stmt and not stmt.startswith(('--', '#')):
                                try:
                                    await cursor.execute(stmt)
                                    logger.info(f"成功执行第 {i + 1} 条SQL语句")
                                except Exception as e:
                                    logger.warning(f"执行第 {i + 1} 条SQL语句时出错: {str(e)}")
                                    logger.warning(f"出错SQL: {stmt[:100]}...")  # 只显示前100个字符
                                    # 可以选择继续执行或抛出异常
                                    # 这里我们选择继续执行下一条语句

                    except Exception as e:
                        logger.warning(f"处理SQL文件 {sql_file} 时出错: {str(e)}")
                        # 继续处理下一个文件
                        continue

                try:
                    await conn.commit()
                    logger.info("所有SQL文件执行完成")
                except Exception as e:
                    logger.warning(f"提交事务时出错: {str(e)}")
                    # 尝试回滚
                    try:
                        await conn.rollback()
                        logger.info("已回滚事务")
                    except Exception as rollback_e:
                        logger.warning(f"回滚事务时出错: {str(rollback_e)}")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                message_str = event.message_str
                if re.search("查询|cx|Query", message_str, re.IGNORECASE) :
                    query_gz = r"(?:查询|cx|Query)\D*(\d+)\D*(\d+)"
                    cxwz = re.search(query_gz, message_str, re.IGNORECASE)
                    try:
                        xinhao = cxwz.group(1)
                        bianhao = cxwz.group(2)
                    except Exception as e:
                        yield event.plain_result(f"您的输入有误，请重新输入。\n该指令应为：指令 型号 编号")
                        return
                    await cursor.execute(f"""SELECT * FROM `设备存放位置` WHERE 设备型号 = %s AND 设备编号 = %s""",
                                         (xinhao, bianhao))
                    chaxun = await cursor.fetchone()
                    if chaxun:
                        chaxunweizhi = chaxun['设备位置']
                        yield event.plain_result(f"型号：{xinhao}\n编号：{bianhao}\n位置：{chaxunweizhi}")
                        return
                    else:
                        yield event.plain_result(f"型号：{xinhao}\n编号：{bianhao}\n位置：位置未知")
                        yield event.plain_result("请在知道设备位置后输入：/添加位置 设备型号-设备编号 设备位置")
                        yield event.plain_result("感谢您的贡献")
                        return
                if re.search("添加位置|tjwz|add location|add loc", message_str, re.IGNORECASE):
                    add_gz = r"(?:添加位置|tjwz|add location|add loc)\D*(\d+)\D*(\d+)\s*([A-Za-z]\d+)"
                    tjwz = re.search(add_gz, message_str, re.IGNORECASE)
                    try:
                        xinhao = tjwz.group(1)
                        bianhao = tjwz.group(2)
                        shebeiweizhi = tjwz.group(3).upper()
                    except Exception as e:
                        yield event.plain_result(f"您的输入有误，请重新输入。\n该指令应为：指令 型号 编号 位置")
                        return
                    await cursor.execute(f"""SELECT * FROM `设备存放位置` WHERE 设备型号 = %s AND 设备编号 = %s""",
                                         (xinhao, bianhao))
                    chaxun = await cursor.fetchone()
                    if chaxun:
                        chaxunweizhi = chaxun['设备位置']
                        yield event.plain_result(f"型号：{xinhao}\n编号：{bianhao}\n位置：{chaxunweizhi}")
                        yield event.plain_result(f"该设备位置已被注册位于{chaxunweizhi}")
                        yield event.plain_result("如果该位置错误请使用：\n/更新位置 设备型号-设备编号 设备位置")
                        yield event.plain_result("感谢您的贡献")
                        return
                    else:
                        # 设备位置写入格式
                        mysql_device_location_write_structure = f"""
                                INSERT INTO `设备存放位置`
                                (
                                    设备型号,
                                    设备编号,
                                    设备位置
                                )
                                    VALUES (%s, %s, %s)
                                """
                        # 设备写入内容
                        mysql_device_location_write_data = (
                            xinhao,
                            bianhao,
                            shebeiweizhi
                        )
                        await cursor.execute(mysql_device_location_write_structure,
                                             mysql_device_location_write_data)
                        yield event.plain_result(f"位置添加成功\n型号：{xinhao}\n编号：{bianhao}\n位置：{shebeiweizhi}")
                        return
                if re.search("更新位置|gxwz|update location|up loc", message_str, re.IGNORECASE):
                    update_gz = r"(?:更新位置|gxwz|update location|up loc)\D*(\d+)\D*(\d+)\s*([A-Za-z]\d+)"
                    gxwz = re.search(update_gz, message_str, re.IGNORECASE)
                    try:
                        xinhao = gxwz.group(1)
                        bianhao = gxwz.group(2)
                        shebeiweizhi = gxwz.group(3).upper()
                    except Exception as e:
                        yield event.plain_result(f"您的输入有误，请重新输入。\n该指令应为：指令 型号 编号 位置")
                        return
                    await cursor.execute(
                        f"""SELECT * FROM `设备存放位置` WHERE 设备型号 = %s AND 设备编号 = %s""",
                        (xinhao, bianhao))
                    chaxun = await cursor.fetchone()
                    if chaxun:
                        # 设备位置写入格式
                        mysql_device_location_update_structure = f"""
                                                    UPDATE `设备存放位置`
                                                    SET 设备位置 = %s
                                                    WHERE 设备型号 = %s AND 设备编号 = %s
                                                    """
                        # 设备写入内容
                        mysql_device_location_update_data = (
                            shebeiweizhi,
                            xinhao,
                            bianhao
                        )
                        await cursor.execute(mysql_device_location_update_structure,
                                             mysql_device_location_update_data)
                        yield event.plain_result(
                            f"位置更新成功\n型号：{xinhao}\n编号：{bianhao}\n位置：{shebeiweizhi}")
                        return
                    else:
                        yield event.plain_result(f"未找到设备 型号：{xinhao} 编号：{bianhao}")
                        yield event.plain_result("请先使用添加位置命令添加设备")
                        return
                else:
                    yield event.plain_result("命令似乎不正确")
                    return

    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""
