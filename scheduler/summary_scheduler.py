import asyncio
from datetime import datetime, timedelta
import pytz
from models.models import get_session, ForwardRule
import logging
import os
from dotenv import load_dotenv
from telethon import TelegramClient, errors
from ai import get_ai_provider
import traceback
from utils.constants import DEFAULT_TIMEZONE,DEFAULT_AI_MODEL,DEFAULT_SUMMARY_PROMPT,DEFAULT_WEEKLY_SUMMARY_PROMPT
from enums.enums import SummaryMode

logger = logging.getLogger(__name__)

# Telegram's maximum message size limit (4096 characters)
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
# Maximum length for each summary message part, leaving headroom for metadata or formatting
MAX_MESSAGE_PART_LENGTH = TELEGRAM_MAX_MESSAGE_LENGTH - 300
# Maximum number of attempts for sending messages
MAX_SEND_ATTEMPTS = 2

class SummaryScheduler:
    def __init__(self, user_client: TelegramClient, bot_client: TelegramClient):
        self.daily_tasks = {}  # 存储每日总结定时任务 {rule_id: task}
        self.weekly_tasks = {}  # 存储周报总结定时任务 {rule_id: task}
        self.timezone = pytz.timezone(DEFAULT_TIMEZONE)
        self.user_client = user_client
        self.bot_client = bot_client
        # 添加信号量来限制并发请求
        self.request_semaphore = asyncio.Semaphore(2)  # 最多同时执行2个请求
        # 从环境变量获取配置
        self.batch_size = int(os.getenv('SUMMARY_BATCH_SIZE', 20))
        self.batch_delay = int(os.getenv('SUMMARY_BATCH_DELAY', 2))

    async def schedule_rule(self, rule):
        """为规则创建或更新定时任务（包括每日总结和周报总结）"""
        try:
            # 调度每日总结任务
            await self._schedule_daily_summary(rule)
            
            # 调度周报总结任务
            await self._schedule_weekly_summary(rule)
            
            logger.info(f"已为规则 {rule.id} 调度所有总结任务")

        except Exception as e:
            logger.error(f"调度规则 {rule.id} 时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
    
    async def _schedule_daily_summary(self, rule):
        """调度每日总结任务"""
        rule_id = rule.id
        
        # 如果任务已存在，先取消
        if rule_id in self.daily_tasks:
            self.daily_tasks[rule_id].cancel()
            del self.daily_tasks[rule_id]
        
        # 如果规则未启用每日总结，直接返回
        if not rule.is_summary:
            return
        
        # 计算下一次运行时间
        next_run_time = self._get_next_run_time(datetime.now(self.timezone), rule.summary_time)
        
        # 创建定时任务
        task = asyncio.create_task(self._run_summary_task(rule))
        self.daily_tasks[rule_id] = task
        
        logger.info(f"已调度规则 {rule_id} 的每日总结任务，下次运行时间: {next_run_time}")
    
    async def _schedule_weekly_summary(self, rule):
        """调度周报总结任务"""
        rule_id = rule.id
        
        # 如果任务已存在，先取消
        if rule_id in self.weekly_tasks:
            self.weekly_tasks[rule_id].cancel()
            del self.weekly_tasks[rule_id]
        
        # 如果规则未启用周报总结，直接返回
        if not hasattr(rule, 'is_weekly_summary') or not rule.is_weekly_summary:
            return
        
        # 计算下一次运行时间
        next_run_time = self._get_next_weekly_run_time(rule.weekly_summary_day, rule.weekly_summary_time)
        
        # 创建定时任务
        task = asyncio.create_task(self._run_summary_task(rule))
        self.weekly_tasks[rule_id] = task
        
        logger.info(f"已调度规则 {rule_id} 的周报总结任务，下次运行时间: {next_run_time}")

    async def _run_summary_task(self, rule, next_run_time: datetime, summary_mode: SummaryMode):
        """运行总结任务的协程"""
        try:
            # 计算等待时间
            now = datetime.now(self.timezone)
            wait_seconds = (next_run_time - now).total_seconds()
            
            mode_name = "每日总结" if summary_mode == SummaryMode.DAILY else "周报总结"
            logger.info(f"规则 {rule.id} 的{mode_name}任务等待 {wait_seconds:.2f} 秒后执行")
            
            # 等待到执行时间
            await asyncio.sleep(wait_seconds)
            
            # 执行总结
            await self._execute_summary(rule.id, summary_mode)
            
            # 重新调度下一次任务
            if summary_mode == SummaryMode.DAILY:
                await self._schedule_daily_summary(rule)
            else:
                await self._schedule_weekly_summary(rule)
            
        except asyncio.CancelledError:
            mode_name = "每日总结" if summary_mode == SummaryMode.DAILY else "周报总结"
            logger.info(f"规则 {rule.id} 的{mode_name}任务已被取消")
        except Exception as e:
            mode_name = "每日总结" if summary_mode == SummaryMode.DAILY else "周报总结"
            logger.error(f"规则 {rule.id} 的{mode_name}任务执行出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            # 出错后重新调度
            await asyncio.sleep(60)  # 等待1分钟后重试
            if summary_mode == SummaryMode.DAILY:
                await self._schedule_daily_summary(rule)
            else:
                await self._schedule_weekly_summary(rule)

    async def _send_summary_message(self, rule: ForwardRule, summary_text: str, summary_mode: SummaryMode = SummaryMode.DAILY):
        """发送总结消息"""
        try:
            # 分割消息
            summary_parts = self._split_message(summary_text)
            
            # 构建消息头
            mode_name = "消息总结" if summary_mode == SummaryMode.DAILY else "周报总结"
            header = f"📋 {rule.source_chat.name} - {mode_name}\n"
            header += f"📊 消息数量: {len(summary_parts)} 部分\n\n"
            
            # 发送消息
            for i, part in enumerate(summary_parts):
                if i == 0:
                    message_to_send = header + part
                else:
                    message_to_send = f"📋 {rule.source_chat.name} - {mode_name}报告 (续 {i+1}/{len(summary_parts)})\n\n" + part
                
                # 发送消息
                await self.bot_client.send_message(rule.destination_chat.chat_id, message_to_send)
                
                # 添加延迟，避免发送过快
                await asyncio.sleep(1)
            
            # 如果需要置顶总结消息
            if rule.is_top_summary:
                try:
                    # 获取最后一条消息并置顶
                    messages = await self.bot_client.get_messages(rule.destination_chat.chat_id, limit=1)
                    if messages:
                        await self.bot_client.pin_message(rule.destination_chat.chat_id, messages[0].id)
                        logger.info(f"已置顶规则 {rule.id} 的{mode_name}消息")
                except Exception as pin_error:
                    logger.warning(f"置顶{mode_name}消息失败: {str(pin_error)}")
            
            logger.info(f"规则 {rule.id} 的{mode_name}消息发送完成")
            
        except Exception as e:
            mode_name = "消息总结" if summary_mode == SummaryMode.DAILY else "周报总结"
            logger.error(f"发送规则 {rule.id} 的{mode_name}消息时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _split_message(self, text: str, max_length: int = MAX_MESSAGE_PART_LENGTH):
        if not text:
            return []

        parts = []
        while len(text) > 0:
            # Strip any leading whitespace from the remaining text to prevent empty parts.
            text = text.lstrip()
            if not text:
                break

            if len(text) <= max_length:
                parts.append(text)
                break

            # Find the best split position, searching backwards from max_length.
            split_pos = -1
            for sep in ('\n\n', '\n', ' '):
                pos = text.rfind(sep, 0, max_length)
                if pos > 0:
                    split_pos = pos
                    break
            if split_pos == -1:
                split_pos = max_length

            parts.append(text[:split_pos])
            text = text[split_pos:]

        return parts

    def _get_next_run_time(self, now, target_time):
        """计算下一次每日总结运行时间"""
        hour, minute = map(int, target_time.split(':'))
        next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if next_time <= now:
            next_time += timedelta(days=1)

        return next_time

    def _get_next_weekly_run_time(self, summary_day, summary_time):
        """计算下一次周报总结运行时间"""
        now = datetime.now(self.timezone)
        
        # 解析时间字符串
        try:
            hour, minute = map(int, summary_time.split(':'))
        except:
            hour, minute = 9, 0  # 默认时间
        
        # 获取今天是星期几（0=周一，6=周日）
        current_weekday = now.weekday()  # 0=周一，6=周日
        target_weekday = summary_day - 1  # 转换为0-based（0=周一，6=周日）
        
        # 计算距离目标星期几还有多少天
        days_ahead = target_weekday - current_weekday
        if days_ahead < 0:
            days_ahead += 7  # 如果目标星期已经过去，安排到下周
        
        # 创建目标日期的运行时间
        target_date = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
        
        # 如果目标时间已经过了，就安排到下周
        if now >= target_date:
            target_date += timedelta(days=7)
        
        return target_date

    async def _execute_summary(self, rule_id, summary_mode: SummaryMode = SummaryMode.DAILY, is_now=False):
        """执行单个规则的总结任务"""
        session = get_session()
        try:
            rule = session.query(ForwardRule).get(rule_id)
            if not is_now:
                if not rule or not rule.is_summary:
                    return

            try:
                source_chat_id = int(rule.source_chat.telegram_chat_id)
                target_chat_id = int(rule.target_chat.telegram_chat_id)

                messages = []

                # 计算时间范围
                now = datetime.now(self.timezone)
                summary_hour, summary_minute = map(int, rule.summary_time.split(':'))

                # 设置结束时间为当前时间
                end_time = now

                # 根据总结模式设置开始时间
                if summary_mode == SummaryMode.DAILY:
                    # 设置开始时间为前一天的总结时间
                    start_time = now.replace(
                        hour=summary_hour,
                        minute=summary_minute,
                        second=0,
                        microsecond=0
                    ) - timedelta(days=1)
                else:  # 周报总结
                    # 设置开始时间为前一周的总结时间
                    start_time = now.replace(
                        hour=summary_hour,
                        minute=summary_minute,
                        second=0,
                        microsecond=0
                    ) - timedelta(days=7)

                mode_name = "每日总结" if summary_mode == SummaryMode.DAILY else "周报总结"
                logger.info(f'规则 {rule_id} 的{mode_name}获取消息时间范围: {start_time} 到 {end_time}')

                async with self.request_semaphore:
                    messages = []
                    current_offset = 0

                    while True:
                        batch = []  # 移到循环外部
                        messages_batch = await self.user_client.get_messages(
                            source_chat_id,
                            limit=self.batch_size,
                            offset_date=end_time,
                            offset_id=current_offset,
                            reverse=False
                        )

                        if not messages_batch:
                            logger.info(f'规则 {rule_id} 没有获取到新消息，退出循环')
                            break

                        logger.info(f'规则 {rule_id} 获取到批次消息数量: {len(messages_batch)}')

                        should_break = False
                        for message in messages_batch:
                            msg_time = message.date.astimezone(self.timezone)
                            preview = message.text[:20] + '...' if message.text else 'None'
                            logger.info(f'规则 {rule_id} 处理消息 - 时间: {msg_time}, 预览: {preview}, 长度: {len(message.text) if message.text else 0}')

                            # 跳过未来时间的消息
                            if msg_time > end_time:
                                continue

                            # 如果消息在有效时间范围内，添加到批次
                            if start_time <= msg_time <= end_time and message.text:
                                batch.append(message.text)

                            # 如果遇到早于开始时间的消息，标记退出
                            if msg_time < start_time:
                                logger.info(f'规则 {rule_id} 消息时间 {msg_time} 早于开始时间 {start_time}，停止获取')
                                should_break = True
                                break

                        # 如果当前批次有消息，添加到总消息列表
                        if batch:
                            messages.extend(batch)
                            logger.info(f'规则 {rule_id} 当前批次添加了 {len(batch)} 条消息，总消息数: {len(messages)}')

                        # 更新offset为最后一条消息的ID
                        current_offset = messages_batch[-1].id

                        # 如果需要退出循环
                        if should_break:
                            break

                        # 在批次之间等待
                        await asyncio.sleep(self.batch_delay)

                if not messages:
                    logger.info(f'规则 {rule_id} 没有需要总结的消息')
                    return

                all_messages = '\n'.join(messages)

                # 检查AI模型设置，如未设置则使用默认模型
                if not rule.ai_model:
                    rule.ai_model = DEFAULT_AI_MODEL
                    logger.info(f"使用默认AI模型进行总结: {rule.ai_model}")
                else:
                    logger.info(f"使用规则配置的AI模型进行总结: {rule.ai_model}")

                # 根据总结模式选择提示词
                if summary_mode == SummaryMode.DAILY:
                    prompt = rule.summary_prompt or DEFAULT_SUMMARY_PROMPT
                else:
                    prompt = rule.summary_prompt or DEFAULT_WEEKLY_SUMMARY_PROMPT

                # 获取AI提供者并处理总结
                provider = await get_ai_provider(rule.ai_model)
                summary = await provider.process_message(
                    all_messages,
                    prompt=prompt,
                    model=rule.ai_model
                )


                if summary:
                    if summary_mode == SummaryMode.DAILY:
                        duration_hours = round((end_time - start_time).total_seconds() / 3600)
                        header = f"📋 {rule.source_chat.name} - {duration_hours}小时消息总结\n"
                    else:
                        duration_days = round((end_time - start_time).total_seconds() / 86400)
                        header = f"📋 {rule.source_chat.name} - {duration_days}天周报总结\n"
                    
                    header += f"🕐 时间范围: {start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')}\n"
                    header += f"📊 消息数量: {len(messages)} 条\n\n"

                    summary_parts = self._split_message(summary, MAX_MESSAGE_PART_LENGTH)

                    summary_message = None
                    for i, part in enumerate(summary_parts):
                        if i == 0:
                            message_to_send = header + part
                        else:
                            if summary_mode == SummaryMode.DAILY:
                                message_to_send = f"📋 {rule.source_chat.name} - 总结报告 (续 {i+1}/{len(summary_parts)})\n\n" + part
                            else:
                                message_to_send = f"📋 {rule.source_chat.name} - 周报总结 (续 {i+1}/{len(summary_parts)})\n\n" + part

                        # 发送消息，支持重试机制
                        current_message = None
                        use_markdown = True
                        attempt = 0

                        while attempt < MAX_SEND_ATTEMPTS:
                            logger.info(f"Retry attempt {attempt + 1}/{MAX_SEND_ATTEMPTS} for sending message to chat ID {target_chat_id}.")
                            try:
                                if use_markdown:
                                    current_message = await self.bot_client.send_message(
                                        target_chat_id,
                                        message_to_send,
                                        parse_mode='markdown'
                                    )
                                else:
                                    # Fallback to plain text
                                    current_message = await self.bot_client.send_message(
                                        target_chat_id,
                                        message_to_send
                                    )
                                break  # Success, exit retry loop

                            except errors.MarkupInvalidError as e:
                                if use_markdown:
                                    logger.warning(f"Markdown解析失败: {e}. 降级为纯文本后重试。")
                                    use_markdown = False
                                    continue  # 立即重试，使用纯文本格式
                                else:
                                    # This should not happen, but if it does, it's a bug.
                                    logger.error(f"纯文本发送时出现意外的 MarkupInvalidError : {e}")
                                    raise # Fail fast

                            except errors.FloodWaitError as fwe:
                                if attempt < MAX_SEND_ATTEMPTS - 1:
                                    logger.warning(f"触发Telegram发送频率限制，等待 {fwe.seconds} 秒后重试...")
                                    await asyncio.sleep(fwe.seconds)
                                    attempt += 1
                                else:
                                    logger.error("重试次数已达上限，发送失败。")
                                    raise

                            except Exception as send_error:
                                logger.error(f"发送总结第 {i+1} 部分时出错: {str(send_error)}")
                                if attempt >= MAX_SEND_ATTEMPTS - 1:
                                    raise # Re-raise on last attempt
                                await asyncio.sleep(1) # Wait a bit before retrying on other errors
                                attempt += 1

                        # 统一处理第一条消息的赋值
                        if i == 0:
                            summary_message = current_message

                    if rule.is_top_summary and summary_message:
                        try:
                            await self.bot_client.pin_message(target_chat_id, summary_message)
                        except Exception as pin_error:
                            logger.warning(f"置顶总结消息失败: {str(pin_error)}")

                    mode_name = "每日总结" if summary_mode == SummaryMode.DAILY else "周报总结"
                    logger.info(f'规则 {rule_id} {mode_name}完成，共处理 {len(messages)} 条消息，分为 {len(summary_parts)} 部分发送')

            except Exception as e:
                mode_name = "每日总结" if summary_mode == SummaryMode.DAILY else "周报总结"
                logger.error(f'执行规则 {rule_id} 的{mode_name}任务时出错: {str(e)}')
                logger.error(f'错误详情: {traceback.format_exc()}')

        finally:
            session.close()

    async def start(self):
        """启动调度器"""
        logger.info("开始启动总结调度器")
        
        # 获取所有启用总结的规则
        session = get_session()
        rules = session.query(ForwardRule).filter(
            (ForwardRule.is_summary == True) | (ForwardRule.is_weekly_summary == True)
        ).all()
        
        # 为每个规则创建任务
        for rule in rules:
            await self.schedule_rule(rule)
        
        logger.info(f"总结调度器启动完成，共调度 {len(rules)} 个规则")

    async def stop(self):
        """停止调度器"""
        logger.info("开始停止总结调度器")
        
        # 取消所有每日总结任务
        for task in self.daily_tasks.values():
            task.cancel()
        
        # 取消所有周报总结任务
        for task in self.weekly_tasks.values():
            task.cancel()
        
        # 等待所有任务完成
        await asyncio.gather(*self.daily_tasks.values(), *self.weekly_tasks.values(), return_exceptions=True)
        
        # 清空任务列表
        self.daily_tasks.clear()
        self.weekly_tasks.clear()
        
        logger.info("总结调度器已停止")

    async def execute_all_summaries(self):
        """立即执行所有总结任务"""
        logger.info("开始立即执行所有总结任务")
        
        # 获取所有启用总结的规则
        session = get_session()
        rules = session.query(ForwardRule).filter(
            (ForwardRule.is_summary == True) | (ForwardRule.is_weekly_summary == True)
        ).all()
        
        # 为每个规则执行总结
        tasks = []
        for rule in rules:
            # 执行每日总结
            if rule.is_summary:
                task = asyncio.create_task(self._execute_summary(rule.id, SummaryMode.DAILY, is_now=True))
                tasks.append(task)
            
            # 执行周报总结
            if rule.is_weekly_summary:
                task = asyncio.create_task(self._execute_summary(rule.id, SummaryMode.WEEKLY, is_now=True))
                tasks.append(task)
        
        # 等待所有任务完成
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"立即执行所有总结任务完成，共处理 {len(tasks)} 个任务")
        session.close()
