from handlers.button.button_helpers import create_delay_time_buttons
from handlers.list_handlers import show_list
from handlers.button.settings_manager import create_settings_text, create_buttons, RULE_SETTINGS, MEDIA_SETTINGS, AI_SETTINGS
from models.models import Chat, ReplaceRule, Keyword,get_session, ForwardRule, RuleSync
from telethon import Button
from handlers.button.callback.ai_callback import *
from handlers.button.callback.media_callback import *
from handlers.button.callback.other_callback import *
from handlers.button.callback.push_callback import *
import logging
import aiohttp
from utils.constants import RSS_HOST, RSS_PORT
from utils.auto_delete import respond_and_delete,reply_and_delete
from utils.common import check_and_clean_chats
from handlers.button.button_helpers import create_sync_rule_buttons,create_other_settings_buttons

logger = logging.getLogger(__name__)


async def callback_switch(event, rule_id, session, message, data):
    """处理切换源聊天的回调"""
    # 获取当前聊天
    current_chat = await event.get_chat()
    current_chat_db = session.query(Chat).filter(
        Chat.telegram_chat_id == str(current_chat.id)
    ).first()

    if not current_chat_db:
        await event.answer('当前聊天不存在')
        return

    # 如果已经选中了这个聊天，就不做任何操作
    if current_chat_db.current_add_id == rule_id:
        await event.answer('已经选中该聊天')
        return

    # 更新当前选中的源聊天
    current_chat_db.current_add_id = rule_id  # 这里的 rule_id 实际上是源聊天的 telegram_chat_id
    session.commit()

    # 更新按钮显示
    rules = session.query(ForwardRule).filter(
        ForwardRule.target_chat_id == current_chat_db.id
    ).all()

    buttons = []
    for rule in rules:
        source_chat = rule.source_chat
        current = source_chat.telegram_chat_id == rule_id
        button_text = f'{"✓ " if current else ""}来自: {source_chat.name}'
        callback_data = f"switch:{source_chat.telegram_chat_id}"
        buttons.append([Button.inline(button_text, callback_data)])

    try:
        await message.edit('请选择要管理的转发规则:', buttons=buttons)
    except Exception as e:
        if 'message was not modified' not in str(e).lower():
            raise  # 如果是其他错误就继续抛出

    source_chat = session.query(Chat).filter(
        Chat.telegram_chat_id == rule_id
    ).first()
    await event.answer(f'已切换到: {source_chat.name if source_chat else "未知聊天"}')

async def callback_settings(event, rule_id, session, message, data):
    """处理显示设置的回调"""
    # 获取当前聊天
    current_chat = await event.get_chat()
    current_chat_db = session.query(Chat).filter(
        Chat.telegram_chat_id == str(current_chat.id)
    ).first()

    if not current_chat_db:
        await event.answer('当前聊天不存在')
        return

    rules = session.query(ForwardRule).filter(
        ForwardRule.target_chat_id == current_chat_db.id
    ).all()

    if not rules:
        await event.answer('当前聊天没有任何转发规则')
        return

    # 创建规则选择按钮
    buttons = []
    for rule in rules:
        source_chat = rule.source_chat
        button_text = f'{source_chat.name}'
        callback_data = f"rule_settings:{rule.id}"
        buttons.append([Button.inline(button_text, callback_data)])

    await message.edit('请选择要管理的转发规则:', buttons=buttons)

async def callback_delete(event, rule_id, session, message, data):
    """处理删除规则的回调"""
    rule = session.query(ForwardRule).get(rule_id)
    if not rule:
        await event.answer('规则不存在')
        return

    try:
        # 先保存规则对象，用于后续检查聊天关联
        rule_obj = rule
        
        # 先删除替换规则
        session.query(ReplaceRule).filter(
            ReplaceRule.rule_id == rule.id
        ).delete()

        # 再删除关键字
        session.query(Keyword).filter(
            Keyword.rule_id == rule.id
        ).delete()

        # 删除规则
        session.delete(rule)
        
        # 提交规则删除的更改
        session.commit()
        
        # 尝试删除RSS服务中的相关数据
        try:
            rss_url = f"http://{RSS_HOST}:{RSS_PORT}/api/rule/{rule_id}"
            async with aiohttp.ClientSession() as client_session:
                async with client_session.delete(rss_url) as response:
                    if response.status == 200:
                        logger.info(f"成功删除RSS规则数据: {rule_id}")
                    else:
                        response_text = await response.text()
                        logger.warning(f"删除RSS规则数据失败 {rule_id}, 状态码: {response.status}, 响应: {response_text}")
        except Exception as rss_err:
            logger.error(f"调用RSS删除API时出错: {str(rss_err)}")
            # 不影响主要流程，继续执行
        
        # 使用通用方法检查并清理不再使用的聊天记录
        deleted_chats = await check_and_clean_chats(session, rule_obj)
        if deleted_chats > 0:
            logger.info(f"删除规则后清理了 {deleted_chats} 个未使用的聊天记录")

        # 删除机器人的消息
        await message.delete()
        # 发送新的通知消息
        await respond_and_delete(event,('✅ 已删除规则'))
        await event.answer('已删除规则')

    except Exception as e:
        session.rollback()
        logger.error(f'删除规则时出错: {str(e)}')
        logger.exception(e)
        await event.answer('删除规则失败，请检查日志')

async def callback_page(event, rule_id, session, message, data):
    """处理翻页的回调"""
    logger.info(f'翻页回调数据: action=page, rule_id={rule_id}')

    try:
        # 解析页码和命令
        page_number, command = rule_id.split(':')
        page = int(page_number)

        # 获取当前聊天和规则
        current_chat = await event.get_chat()
        current_chat_db = session.query(Chat).filter(
            Chat.telegram_chat_id == str(current_chat.id)
        ).first()

        if not current_chat_db or not current_chat_db.current_add_id:
            await event.answer('请先选择一个源聊天')
            return

        source_chat = session.query(Chat).filter(
            Chat.telegram_chat_id == current_chat_db.current_add_id
        ).first()

        rule = session.query(ForwardRule).filter(
            ForwardRule.source_chat_id == source_chat.id,
            ForwardRule.target_chat_id == current_chat_db.id
        ).first()

        if command == 'keyword':
            # 获取关键字列表
            keywords = session.query(Keyword).filter(
                Keyword.rule_id == rule.id
            ).all()

            await show_list(
                event,
                'keyword',
                keywords,
                lambda i, kw: f'{i}. {kw.keyword}{" (正则)" if kw.is_regex else ""}',
                f'关键字列表\n规则: 来自 {source_chat.name}',
                page
            )

        elif command == 'replace':
            # 获取替换规则列表
            replace_rules = session.query(ReplaceRule).filter(
                ReplaceRule.rule_id == rule.id
            ).all()

            await show_list(
                event,
                'replace',
                replace_rules,
                lambda i, rr: f'{i}. 匹配: {rr.pattern} -> {"删除" if not rr.content else f"替换为: {rr.content}"}',
                f'替换规则列表\n规则: 来自 {source_chat.name}',
                page
            )

        # 标记回调已处理
        await event.answer()

    except Exception as e:
        logger.error(f'处理翻页时出错: {str(e)}')
        await event.answer('处理翻页时出错，请检查日志')



async def callback_rule_settings(event, rule_id, session, message, data):
    """处理规则设置的回调"""
    rule = session.query(ForwardRule).get(rule_id)
    if not rule:
        await event.answer('规则不存在')
        return

    await message.edit(
        await create_settings_text(rule),
        buttons=await create_buttons(rule)
    )

async def callback_toggle_current(event, rule_id, session, message, data):
    """处理切换当前规则的回调"""
    rule = session.query(ForwardRule).get(rule_id)
    if not rule:
        await event.answer('规则不存在')
        return

    target_chat = rule.target_chat
    source_chat = rule.source_chat

    # 检查是否已经是当前选中的规则
    if target_chat.current_add_id == source_chat.telegram_chat_id:
        await event.answer('已经是当前选中的规则')
        return

    # 更新当前选中的源聊天
    target_chat.current_add_id = source_chat.telegram_chat_id
    session.commit()

    # 更新按钮显示
    try:
        await message.edit(
            await create_settings_text(rule),
            buttons=await create_buttons(rule)
        )
    except Exception as e:
        if 'message was not modified' not in str(e).lower():
            raise

    await event.answer(f'已切换到: {source_chat.name}')



async def callback_set_delay_time(event, rule_id, session, message, data):
    await event.edit("请选择延迟时间：", buttons=await create_delay_time_buttons(rule_id, page=0))
    return



async def callback_delay_time_page(event, rule_id, session, message, data):
    _, rule_id, page = data.split(':')
    page = int(page)
    await event.edit("请选择延迟时间：", buttons=await create_delay_time_buttons(rule_id, page=page))
    return

            


async def callback_select_delay_time(event, rule_id, session, message, data):
    parts = data.split(':', 2)  # 最多分割2次
    if len(parts) == 3:
        _, rule_id, time = parts
        logger.info(f"设置规则 {rule_id} 的延迟时间为: {time}")
        try:
            rule = session.query(ForwardRule).get(int(rule_id))
            if rule:
                # 记录旧时间
                old_time = rule.delay_seconds

                # 更新时间
                rule.delay_seconds = int(time)
                session.commit()
                logger.info(f"数据库更新成功: {old_time} -> {time}")

                # 获取消息对象
                message = await event.get_message()

                await message.edit(
                    await create_settings_text(rule),
                    buttons=await create_buttons(rule)
                )
                logger.info("界面更新完成")
        except Exception as e:
            logger.error(f"设置延迟时间时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
        finally:
            session.close()
    return

async def callback_set_sync_rule(event, rule_id, session, message, data):
    """处理设置同步规则的回调"""
    try:
        rule = session.query(ForwardRule).get(int(rule_id))
        if not rule:
            await event.answer('规则不存在')
            return
        
        await message.edit("请选择要同步到的规则：", buttons=await create_sync_rule_buttons(rule_id, page=0))
    except Exception as e:
        logger.error(f"设置同步规则时出错: {str(e)}")
        await event.answer('处理请求时出错，请检查日志')
    return

async def callback_toggle_rule_sync(event, rule_id_data, session, message, data):
    """处理切换规则同步状态的回调"""
    try:
        # 解析回调数据 - 格式为 source_rule_id:target_rule_id:page
        parts = rule_id_data.split(":")
        if len(parts) != 3:
            await event.answer('回调数据格式错误')
            return
        
        source_rule_id = int(parts[0])
        target_rule_id = int(parts[1])
        page = int(parts[2])
        
        # 获取数据库操作对象
        db_ops = await get_db_ops()
        
        # 检查是否已存在同步关系
        syncs = await db_ops.get_rule_syncs(session, source_rule_id)
        sync_target_ids = [sync.sync_rule_id for sync in syncs]
        
        # 切换同步状态
        if target_rule_id in sync_target_ids:
            # 如果已同步，则删除同步关系
            success, message_text = await db_ops.delete_rule_sync(session, source_rule_id, target_rule_id)
            if success:
                await event.answer(f'已取消同步规则 {target_rule_id}')
            else:
                await event.answer(f'取消同步失败: {message_text}')
        else:
            # 如果未同步，则添加同步关系
            success, message_text = await db_ops.add_rule_sync(session, source_rule_id, target_rule_id)
            if success:
                await event.answer(f'已设置同步到规则 {target_rule_id}')
            else:
                await event.answer(f'设置同步失败: {message_text}')
        
        # 更新按钮显示
        await message.edit("请选择要同步到的规则：", buttons=await create_sync_rule_buttons(source_rule_id, page))
        
    except Exception as e:
        logger.error(f"切换规则同步状态时出错: {str(e)}")
        await event.answer('处理请求时出错，请检查日志')
    return

async def callback_sync_rule_page(event, rule_id_data, session, message, data):
    """处理同步规则页面的翻页功能"""
    try:
        # 解析回调数据 - 格式为 rule_id:page
        parts = rule_id_data.split(":")
        if len(parts) != 2:
            await event.answer('回调数据格式错误')
            return
        
        rule_id = int(parts[0])
        page = int(parts[1])
        
        # 检查规则是否存在
        rule = session.query(ForwardRule).get(rule_id)
        if not rule:
            await event.answer('规则不存在')
            return
        
        # 更新按钮显示
        await message.edit("请选择要同步到的规则：", buttons=await create_sync_rule_buttons(rule_id, page))
        
    except Exception as e:
        logger.error(f"处理同步规则页面翻页时出错: {str(e)}")
        await event.answer('处理请求时出错，请检查日志')
    return


async def callback_close_settings(event, rule_id, session, message, data):
    """处理关闭设置按钮的回调，删除当前消息"""
    try:
        logger.info("执行关闭设置操作，准备删除消息")
        await message.delete()
    except Exception as e:
        logger.error(f"删除消息时出错: {str(e)}")
        await event.answer("关闭设置失败，请检查日志")

async def callback_noop(event, rule_id, session, message, data):
    # 用于页码按钮，不做任何操作
    await event.answer("当前页码")
    return


async def callback_page_rule(event, page_str, session, message, data):
    """处理规则列表分页的回调"""
    try:
        page = int(page_str)
        if page < 1:
            await event.answer('已经是第一页了')
            return

        per_page = 30
        offset = (page - 1) * per_page

        # 获取总规则数
        total_rules = session.query(ForwardRule).count()
        
        if total_rules == 0:
            await event.answer('没有任何规则')
            return

        # 计算总页数
        total_pages = (total_rules + per_page - 1) // per_page

        if page > total_pages:
            await event.answer('已经是最后一页了')
            return

        # 获取当前页的规则
        rules = session.query(ForwardRule).order_by(ForwardRule.id).offset(offset).limit(per_page).all()
            
        # 构建规则列表消息
        message_parts = [f'📋 转发规则列表 (第{page}/{total_pages}页)：\n']
        
        for rule in rules:
            source_chat = rule.source_chat
            target_chat = rule.target_chat
            
            rule_desc = (
                f'<b>ID: {rule.id}</b>\n'
                f'<blockquote>来源: {source_chat.name} ({source_chat.telegram_chat_id})\n'
                f'目标: {target_chat.name} ({target_chat.telegram_chat_id})\n'
                '</blockquote>'
            )
            message_parts.append(rule_desc)

        # 创建分页按钮
        buttons = []
        nav_row = []

        if page > 1:
            nav_row.append(Button.inline('⬅️ 上一页', f'page_rule:{page-1}'))
        else:
            nav_row.append(Button.inline('⬅️', 'noop'))

        nav_row.append(Button.inline(f'{page}/{total_pages}', 'noop'))

        if page < total_pages:
            nav_row.append(Button.inline('下一页 ➡️', f'page_rule:{page+1}'))
        else:
            nav_row.append(Button.inline('➡️', 'noop'))

        buttons.append(nav_row)

        await message.edit('\n'.join(message_parts), buttons=buttons, parse_mode='html')
        await event.answer()

    except Exception as e:
        logger.error(f'处理规则列表分页时出错: {str(e)}')
        await event.answer('处理分页请求时出错，请检查日志')

async def update_rule_setting(event, rule_id, session, message, field_name, config, setting_type):
    """通用的规则设置更新函数
    
    Args:
        event: 回调事件
        rule_id: 规则ID
        session: 数据库会话
        message: 消息对象
        field_name: 字段名
        config: 设置配置
        setting_type: 设置类型 ('rule', 'media', 'ai')
    """
    logger.info(f'找到匹配的设置项: {field_name}')
    rule = session.query(ForwardRule).get(int(rule_id))
    if not rule:
        logger.warning(f'规则不存在: {rule_id}')
        await event.answer('规则不存在')
        return False

    current_value = getattr(rule, field_name)
    new_value = config['toggle_func'](current_value)
    setattr(rule, field_name, new_value)

    try:
        # 首先更新当前规则
        session.commit()
        logger.info(f'更新规则 {rule.id} 的 {field_name} 从 {current_value} 到 {new_value}')

        # 检查是否启用了同步功能，且不是"是否启用规则"字段和"启用同步"字段
        if rule.enable_sync and field_name != 'enable_rule' and field_name != 'enable_sync':
            logger.info(f"规则 {rule.id} 启用了同步功能，正在同步设置更改到关联规则")
            # 获取需要同步的规则列表
            sync_rules = session.query(RuleSync).filter(RuleSync.rule_id == rule.id).all()
            
            # 为每个同步规则应用相同的设置
            for sync_rule in sync_rules:
                sync_rule_id = sync_rule.sync_rule_id
                logger.info(f"正在同步设置 {field_name} 到规则 {sync_rule_id}")
                
                # 获取同步目标规则
                target_rule = session.query(ForwardRule).get(sync_rule_id)
                if not target_rule:
                    logger.warning(f"同步目标规则 {sync_rule_id} 不存在，跳过")
                    continue
                
                # 更新同步目标规则的设置
                try:
                    # 记录旧值
                    old_value = getattr(target_rule, field_name)
                    
                    # 设置新值
                    setattr(target_rule, field_name, new_value)
                    session.flush()
                    
                    logger.info(f"同步规则 {sync_rule_id} 的 {field_name} 从 {old_value} 到 {new_value}")
                except Exception as e:
                    logger.error(f"同步设置到规则 {sync_rule_id} 时出错: {str(e)}")
                    continue
            
            # 提交所有同步更改
            session.commit()
            logger.info("所有同步更改已提交")

        # 根据设置类型更新UI
        if setting_type == 'rule':
            await message.edit(
                await create_settings_text(rule),
                buttons=await create_buttons(rule)
            )
        elif setting_type == 'media':
            await event.edit("媒体设置：", buttons=await create_media_settings_buttons(rule))
        elif setting_type == 'ai':
            await message.edit(
                await get_ai_settings_text(rule),
                buttons=await create_ai_settings_buttons(rule)
            )
        elif setting_type == 'other':
            await event.edit("其他设置：", buttons=await create_other_settings_buttons(rule))
        elif setting_type == 'push':
            await event.edit(PUSH_SETTINGS_TEXT, buttons=await create_push_settings_buttons(rule), link_preview=False)
        display_name = config.get('display_name', field_name)
        if field_name == 'use_bot':
            await event.answer(f'已切换到{"机器人" if new_value else "用户账号"}模式')
        else:
            await event.answer(f'已更新{display_name}')
        return True
    except Exception as e:
        session.rollback()
        logger.error(f'更新规则设置时出错: {str(e)}')
        await event.answer('更新设置失败，请检查日志')
        return False


async def handle_callback(event):
    """处理按钮回调"""
    try:
        data = event.data.decode()
        logger.info(f'收到回调数据: {data}')

        # 解析回调数据
        parts = data.split(':')
        action = parts[0]
        rule_id = ':'.join(parts[1:]) if len(parts) > 1 else None
        logger.info(f'解析回调数据: action={action}, rule_id={rule_id}')

        # 获取消息对象
        message = await event.get_message()

        # 使用会话
        session = get_session()
        try:  
            # 获取对应的处理器
            handler = CALLBACK_HANDLERS.get(action)
            if handler:
                logger.info(f'找到对应的处理器: {handler}')
                await handler(event, rule_id, session, message, data)
            else:
                logger.info(f'未找到对应的处理器,尝试处理规则设置切换: {action}')
                
                # 尝试在RULE_SETTINGS中查找
                for field_name, config in RULE_SETTINGS.items():
                    if action == config['toggle_action']:
                        success = await update_rule_setting(event, rule_id, session, message, field_name, config, 'rule')
                        if success:
                            return

                # 尝试在MEDIA_SETTINGS中查找
                for field_name, config in MEDIA_SETTINGS.items():
                    if action == config['toggle_action']:
                        success = await update_rule_setting(event, rule_id, session, message, field_name, config, 'media')
                        if success:
                            return

                # 尝试在AI_SETTINGS中查找
                for field_name, config in AI_SETTINGS.items():
                    if action == config['toggle_action']:
                        success = await update_rule_setting(event, rule_id, session, message, field_name, config, 'ai')
                        if success:
                            return
        finally:
            session.close()

    except Exception as e:
        logger.error(f'处理按钮回调时出错: {str(e)}')
        logger.error(f'错误堆栈: {traceback.format_exc()}')
        await event.answer('处理请求时出错，请检查日志')



# 回调处理器字典
CALLBACK_HANDLERS = {
    'toggle_current': callback_toggle_current,
    'switch': callback_switch,
    'settings': callback_settings,
    'delete': callback_delete,
    'page': callback_page,
    'rule_settings': callback_rule_settings,
    'set_summary_time': callback_set_summary_time,
    'set_delay_time': callback_set_delay_time,
    'select_delay_time': callback_select_delay_time,
    'delay_time_page': callback_delay_time_page,
    'page_rule': callback_page_rule,
    'close_settings': callback_close_settings,
    'set_sync_rule': callback_set_sync_rule,
    'toggle_rule_sync': callback_toggle_rule_sync,
    'sync_rule_page': callback_sync_rule_page,
    # AI设置
    'set_summary_prompt': callback_set_summary_prompt,
    'set_ai_prompt': callback_set_ai_prompt,
    'ai_settings': callback_ai_settings,
    'time_page': callback_time_page,
    'select_time': callback_select_time,
    'select_model': callback_select_model,
    'model_page': callback_model_page,
    'change_model': callback_change_model,
    'cancel_set_prompt': callback_cancel_set_prompt,
    'cancel_set_summary': callback_cancel_set_summary,
    'summary_now':callback_summary_now,
    # 周报总结设置
    'toggle_weekly_summary': callback_toggle_weekly_summary,
    'set_weekly_summary_day': callback_set_weekly_summary_day,
    'select_weekly_summary_day': callback_select_weekly_summary_day,
    'set_weekly_summary_time': callback_set_weekly_summary_time,
    'select_weekly_summary_time': callback_select_weekly_summary_time,
    'weekly_time_page': callback_weekly_time_page,
    # 媒体设置
    'select_max_media_size': callback_select_max_media_size,
    'set_max_media_size': callback_set_max_media_size,
    'media_settings': callback_media_settings,
    'set_media_types': callback_set_media_types,
    'toggle_media_type': callback_toggle_media_type,
    'set_media_extensions': callback_set_media_extensions,
    'media_extensions_page': callback_media_extensions_page,
    'toggle_media_extension': callback_toggle_media_extension,
    'toggle_media_allow_text': callback_toggle_media_allow_text,
    'noop': callback_noop,
    # 其他设置
    'other_settings': callback_other_settings,
    'copy_rule': callback_copy_rule,
    'copy_keyword': callback_copy_keyword,
    'copy_replace': callback_copy_replace,
    'clear_keyword': callback_clear_keyword,
    'clear_replace': callback_clear_replace,
    'delete_rule': callback_delete_rule,
    'perform_copy_rule': callback_perform_copy_rule,
    'perform_copy_keyword': callback_perform_copy_keyword,
    'perform_copy_replace': callback_perform_copy_replace,
    'perform_clear_keyword': callback_perform_clear_keyword,
    'perform_clear_replace': callback_perform_clear_replace,
    'perform_delete_rule': callback_perform_delete_rule,
    'set_userinfo_template': callback_set_userinfo_template,
    'set_time_template': callback_set_time_template,
    'set_original_link_template': callback_set_original_link_template,
    'cancel_set_userinfo': callback_cancel_set_userinfo,
    'cancel_set_time': callback_cancel_set_time,
    'cancel_set_original_link': callback_cancel_set_original_link,
    'toggle_reverse_blacklist': callback_toggle_reverse_blacklist,
    'toggle_reverse_whitelist': callback_toggle_reverse_whitelist,
    # 推送设置
    'push_settings': callback_push_settings,
    'toggle_enable_push': callback_toggle_enable_push,
    'toggle_enable_only_push': callback_toggle_enable_only_push,
    'add_push_channel': callback_add_push_channel,
    'cancel_add_push_channel': callback_cancel_add_push_channel,
    'toggle_push_config': callback_toggle_push_config,
    'toggle_push_config_status': callback_toggle_push_config_status,
    'toggle_media_send_mode': callback_toggle_media_send_mode,
    'delete_push_config': callback_delete_push_config,
    'push_page': callback_push_page,
}
