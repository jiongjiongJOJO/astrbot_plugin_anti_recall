# main.py

import asyncio
import json
import time
import pickle
from pathlib import Path
from collections import OrderedDict
from .utils import delete_file, delayed_delete
from astrbot.api import logger
from astrbot.api import AstrBotConfig
from astrbot.api.star import StarTools
from astrbot.api import message_components as Comp
from astrbot.api.star import Context, Star, register
from astrbot.core.message.message_event_result import MessageChain
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent

@register("astrbot_plugin_anti_recall", "JOJO",
          "[仅限aiocqhttp] 防撤回插件，开启监控指定会话后，该会话内撤回的消息将转发给指定接收者", "0.0.6")
class AntiRecall(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.config = config
        if isinstance(self.config.get("message_forward"), str):
            self.config["message_forward"] = json.loads(
                self.config.get("message_forward", "[]"), strict=False
            )
        logger.info('[防撤回插件] 成功加载配置: {}'.format(self.config))

        self.temp_path = Path(StarTools.get_data_dir()) / "anti_recall_cache"
        self.temp_path.mkdir(exist_ok=True)

        self.message_cache = OrderedDict()
        self.max_cache_size = 1000
        self.cache_expire_time = 30 * 60

        self.pending_recalls = OrderedDict()
        self.max_pending_recalls = 100

        current_time = time.time() * 1000
        cleaned_count = 0
        for file in self.temp_path.glob("*.pkl"):
            try:
                file_create_time = int(file.name.split('_')[0])
                if current_time - file_create_time > self.cache_expire_time * 1000:
                    delete_file(file)
                    cleaned_count += 1
            except (ValueError, IndexError):
                delete_file(file)
                cleaned_count += 1
        logger.info(f'[防撤回插件] 清理临时目录完成，共清理 {cleaned_count} 个过期文件')

    def get_origin_list(self):
        message_forward = self.config.get("message_forward", [])
        return [task.get("message_origin") for task in message_forward if isinstance(task, dict) and "message_origin" in task]

    def get_forward_to_list(self, group_id: str):
        message_forward = self.config.get("message_forward", [])
        for task in message_forward:
            if isinstance(task, dict) and task.get("message_origin") == group_id:
                return task.get("forward_to", [])
        return []

    def add_to_cache(self, group_id: str, message_id: str, message):
        cache_key = (group_id, message_id)
        current_time = time.time()
        logger.debug(f"[防撤回|CACHE] ADD: key={cache_key}, content_len={len(message) if message is not None else 'N/A'}")
        if len(self.message_cache) >= self.max_cache_size:
            self.message_cache.popitem(last=False)
        self.message_cache[cache_key] = (current_time, message)
        self.message_cache.move_to_end(cache_key)
        self._clean_expired_cache()

    def get_from_cache(self, group_id: str, message_id: str):
        cache_key = (group_id, message_id)
        if cache_key in self.message_cache:
            timestamp, message = self.message_cache[cache_key]
            current_time = time.time()
            if current_time - timestamp <= self.cache_expire_time:
                self.message_cache.move_to_end(cache_key)
                return message
            else:
                del self.message_cache[cache_key]
        return None

    def _clean_expired_cache(self):
        current_time = time.time()
        expired_keys = [key for key, (timestamp, _) in self.message_cache.items() if current_time - timestamp > self.cache_expire_time]
        for key in expired_keys:
            if key in self.message_cache:
                del self.message_cache[key]

    def add_pending_recall(self, group_id: str, message_id: str, user_id: str, forward_to_list):
        cache_key = (group_id, message_id)
        current_time = time.time()
        if len(self.pending_recalls) >= self.max_pending_recalls:
            self.pending_recalls.popitem(last=False)
        self.pending_recalls[cache_key] = (current_time, user_id, forward_to_list)
        self.pending_recalls.move_to_end(cache_key)
        logger.info(f'[防撤回插件] 添加待处理撤回通知: group_id={group_id}, message_id={message_id}')

    def clean_expired_pending_recalls(self):
        current_time = time.time()
        expire_time = 60
        expired_keys = [key for key, (timestamp, _, _) in self.pending_recalls.items() if current_time - timestamp > expire_time]
        for key in expired_keys:
            if key in self.pending_recalls:
                del self.pending_recalls[key]

    def find_message_file(self, group_id: str, message_id: str):
        current_time = time.time() * 1000
        time_range = self.cache_expire_time * 1000
        matching_files = []
        for file in self.temp_path.glob(f"*_{group_id}_{message_id}.pkl"):
            try:
                file_time = int(file.name.split('_')[0])
                if current_time - file_time <= time_range:
                    matching_files.append((file_time, file))
            except (ValueError, IndexError):
                continue
        if matching_files:
            matching_files.sort(reverse=True)
            return matching_files[0][1]
        return None

    def _parse_onebot_segment(self, segment: dict):
        seg_type = segment.get("type")
        data = segment.get("data", {})
        if seg_type == "text": return Comp.Plain(data.get("text", ""))
        if seg_type == "image": return Comp.Image.fromURL(data.get("url", ""))
        if seg_type == "at": return Comp.At(qq=data.get("qq", ""))
        if seg_type == "face": return Comp.Face(id=int(data.get("id", 0)))
        if seg_type == "reply": return Comp.Reply(id=data.get("id", ""))
        return None

    def _parse_raw_nodes_to_astrbot_nodes(self, raw_nodes: list) -> list[Comp.Node]:
        astrbot_nodes = []
        for node in raw_nodes:
            sender_id = node.get("sender", {}).get("user_id")
            sender_name = node.get("sender", {}).get("nickname")
            content_chain = []
            
            # 关键修复: 从 "message" 键获取内容, 而不是 "content"
            raw_content = node.get("message", [])
            
            if isinstance(raw_content, str):
                content_chain.append(Comp.Plain(raw_content))
            elif isinstance(raw_content, list):
                for segment in raw_content:
                    comp = self._parse_onebot_segment(segment)
                    if comp: content_chain.append(comp)
            astrbot_nodes.append(Comp.Node(uin=sender_id, name=sender_name, content=content_chain))
        return astrbot_nodes
    
    def _convert_astrbot_component_to_raw(self, component) -> dict:
        if isinstance(component, Comp.Plain):
            return {"type": "text", "data": {"text": component.text}}
        if isinstance(component, Comp.Image):
            return {"type": "image", "data": {"file": component.url}}
        if isinstance(component, Comp.At):
            return {"type": "at", "data": {"qq": str(component.qq)}}
        if isinstance(component, Comp.Face):
            return {"type": "face", "data": {"id": str(component.id)}}
        if isinstance(component, Comp.Reply):
            return {"type": "reply", "data": {"id": str(component.id)}}
        return {}

    def _convert_astrbot_nodes_to_raw(self, astrbot_nodes: list[Comp.Node]) -> list[dict]:
        raw_nodes = []
        for node in astrbot_nodes:
            raw_content = [self._convert_astrbot_component_to_raw(comp) for comp in node.content]
            raw_nodes.append({
                "type": "node",
                "data": {
                    "user_id": str(node.uin),
                    "nickname": node.name,
                    # 发送时使用 "content" 键
                    "content": [c for c in raw_content if c]
                }
            })
        return raw_nodes
    
    def _validate_and_normalize_session_string(self, session_str: str, default_platform: str = "aiocqhttp") -> str:
        """
        验证并规范化会话字符串为正确格式: platform:MessageType:session_id
        
        Args:
            session_str: 输入的会话字符串
            default_platform: 默认平台 (默认: aiocqhttp)
            
        Returns:
            规范化后的会话字符串，格式为 platform:MessageType:session_id
            如果格式已经正确，返回原字符串
            如果格式不正确但可以推断，返回规范化后的字符串
            如果无法处理或输入为空，返回 None
        """
        if not session_str:
            return None
            
        parts = session_str.split(':')
        
        # 已经是正确格式: platform:MessageType:session_id
        if len(parts) == 3:
            platform, msg_type, session_id = parts
            # 验证 MessageType 是否有效
            if msg_type in ['GroupMessage', 'FriendMessage', 'OtherMessage']:
                return session_str
            else:
                logger.warning(f"[防撤回插件] 无效的消息类型: {msg_type}，会话字符串: {session_str}")
                return None
        
        # 只有两部分: MessageType:session_id，添加默认平台
        elif len(parts) == 2:
            msg_type, session_id = parts
            if msg_type in ['GroupMessage', 'FriendMessage', 'OtherMessage']:
                normalized = f"{default_platform}:{msg_type}:{session_id}"
                logger.info(f"[防撤回插件] 自动规范化会话字符串: {session_str} -> {normalized}")
                return normalized
            else:
                logger.warning(f"[防撤回插件] 无法识别的会话字符串格式: {session_str}")
                return None
        
        # 只有一个部分: session_id，尝试推断为好友消息
        elif len(parts) == 1:
            session_id = parts[0]
            # 假设纯数字ID默认为好友消息
            if session_id.isdigit():
                normalized = f"{default_platform}:FriendMessage:{session_id}"
                logger.info(f"[防撤回插件] 自动规范化会话字符串 (假设为好友消息): {session_str} -> {normalized}")
                return normalized
            else:
                logger.warning(f"[防撤回插件] 无法识别的会话字符串格式: {session_str}")
                return None
        
        else:
            logger.warning(f"[防撤回插件] 无效的会话字符串格式: {session_str}")
            return None

    async def _send_recall_notification(self, user_id: str, group_id: str, recalled_content: list, forward_to_list: list, bot_id: str):
        is_forward_content = recalled_content is not None and all(isinstance(comp, Comp.Node) for comp in recalled_content)

        if not is_forward_content:
            final_message_chain = [Comp.Plain(f'用户: {user_id} 在群组 {group_id} 撤回了消息:\n\n')] + (recalled_content or [])
            for forward_to in forward_to_list:
                try:
                    # 验证并规范化会话字符串
                    normalized_session = self._validate_and_normalize_session_string(forward_to)
                    if not normalized_session:
                        logger.error(f'[防撤回插件] 跳过无效的会话字符串: {forward_to}')
                        continue
                    
                    await self.context.send_message(normalized_session, MessageChain(chain=final_message_chain))
                    logger.info(f'[防撤回插件] 成功转发普通消息到: {normalized_session}')
                except Exception as e:
                    logger.error(f'[防撤回插件] 转发普通消息失败 to {forward_to}: {e}')
            return

        platform = self.context.get_platform(filter.PlatformAdapterType.AIOCQHTTP)
        if not platform:
            logger.error("[防撤回插件] 无法获取 aiocqhttp 平台实例。")
            return
        
        client = platform.get_client()
        if not client:
            logger.error("[防撤回插件] 无法从平台实例获取客户端。")
            return

        notification_node = Comp.Node(
            uin=bot_id, name="防撤回通知",
            content=[Comp.Plain(f"用户 {user_id} 在群 {group_id} 撤回了合并转发消息:")]
        )
        final_nodes_astrbot = [notification_node] + recalled_content
        final_nodes_raw = self._convert_astrbot_nodes_to_raw(final_nodes_astrbot)
        
        for forward_to_umo in forward_to_list:
            try:
                # 验证并规范化会话字符串
                normalized_session = self._validate_and_normalize_session_string(forward_to_umo)
                if not normalized_session:
                    logger.error(f'[防撤回插件] 跳过无效的会话字符串: {forward_to_umo}')
                    continue
                
                # 验证函数保证返回的格式一定是 platform:MessageType:session_id
                parts = normalized_session.split(':')
                target_type = parts[1]
                target_id = int(parts[2])

                if target_type == "GroupMessage":
                    await client.api.call_action('send_group_forward_msg', group_id=target_id, messages=final_nodes_raw)
                    logger.info(f"[防撤回|SEND] 成功发送合并转发到群聊: {target_id}")
                elif target_type == "FriendMessage":
                    await client.api.call_action('send_private_forward_msg', user_id=target_id, messages=final_nodes_raw)
                    logger.info(f"[防撤回|SEND] 成功发送合并转发到好友: {target_id}")

            except Exception as e:
                logger.error(f'[防撤回插件] 通过底层API转发合并消息失败 to {forward_to_umo}: {e}')

    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def on_all_message(self, event: AstrMessageEvent):
        if not isinstance(event, AiocqhttpMessageEvent): return

        raw_message = event.message_obj.raw_message
        group_id = event.get_group_id()
        if not group_id or group_id not in self.get_origin_list():
            return
        
        message_name = raw_message.name

        if message_name == 'message.group.normal':
            message_id = str(raw_message.message_id)
            message_to_cache = event.get_messages()

            if message_to_cache and isinstance(message_to_cache[0], Comp.Forward):
                forward_id = message_to_cache[0].id
                client = event.bot
                try:
                    raw_forward_content = await client.api.call_action('get_forward_msg', id=forward_id)
                    if raw_forward_content and 'messages' in raw_forward_content:
                        message_to_cache = self._parse_raw_nodes_to_astrbot_nodes(raw_forward_content['messages'])
                        logger.debug(f"[防撤回插件] 成功主动缓存合并转发消息内容 Group({group_id}) MsgID({message_id})")
                    else:
                        logger.warning(f"[防撤回插件] 主动获取合并转发消息 ({forward_id}) 内容失败，API未返回有效数据。")
                except Exception as e:
                    logger.error(f"[防撤回插件] 主动获取合并转发消息 ({forward_id}) 内容时出错: {e}")
            
            self.add_to_cache(group_id, message_id, message_to_cache)
            file_name = f'{int(time.time() * 1000)}_{group_id}_{message_id}.pkl'
            file_path = self.temp_path / file_name
            try:
                with open(file_path, 'wb') as f: pickle.dump(message_to_cache, f)
                asyncio.create_task(delayed_delete(self.cache_expire_time, file_path))
            except Exception as e:
                logger.error(f"[防撤回插件] 写入缓存文件失败: {e}")

            cache_key = (group_id, message_id)
            if cache_key in self.pending_recalls:
                _, user_id, forward_to_list = self.pending_recalls.pop(cache_key)
                logger.info(f'[防撤回插件] 消息到达后立即处理待处理撤回: group_id={group_id}, message_id={message_id}')
                await self._send_recall_notification(user_id, group_id, message_to_cache, forward_to_list, event.message_obj.self_id)
            
            self.clean_expired_pending_recalls()

        elif message_name == 'notice.group_recall':
            recalled_message_id = str(raw_message.message_id)
            user_id = str(raw_message.user_id)
            forward_to_list = self.get_forward_to_list(group_id)

            message_content = self.get_from_cache(group_id, recalled_message_id)
            
            if message_content is not None:
                source = "内存缓存"
                logger.info(f'[防撤回插件] 用户: {user_id} 在群组 {group_id} 内撤回了消息 (来源: {source})')
                await self._send_recall_notification(user_id, group_id, message_content, forward_to_list, event.message_obj.self_id)
            else:
                message_content_from_file = None
                file_path = self.find_message_file(group_id, recalled_message_id)
                if file_path and file_path.exists():
                    try:
                        with open(file_path, 'rb') as f: message_content_from_file = pickle.load(f)
                    except Exception as e:
                        logger.error(f'[防撤回插件] 读取消息文件失败: {e}')
                
                if message_content_from_file is not None:
                    source = "文件缓存"
                    logger.info(f'[防撤回插件] 用户: {user_id} 在群组 {group_id} 内撤回了消息 (来源: {source})')
                    await self._send_recall_notification(user_id, group_id, message_content_from_file, forward_to_list, event.message_obj.self_id)
                else:
                    logger.info(f'[防撤回插件] 内存和文件中均找不到撤回消息的记录，添加到待处理队列: group_id={group_id}, message_id={recalled_message_id}')
                    self.add_pending_recall(group_id, recalled_message_id, user_id, forward_to_list)

    @filter.command_group("防撤回", alias={'anti_recall'})
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def anti_recall(self):
        pass

    @anti_recall.command("增加", alias={'添加', 'add'})
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def add_anti_recall_task(self, event: AstrMessageEvent, group_id: str, user_list: str):
        user_sids = [user.strip() for user in user_list.split(',')]
        message_forward = self.config.get("message_forward", [])
        task_found = False
        for task in message_forward:
            if task.get("message_origin") == group_id:
                task_found = True
                existing_users = set(task.get("forward_to", []))
                existing_users.update(user_sids)
                task["forward_to"] = sorted(list(existing_users))
                break
        if not task_found:
            message_forward.append({
                "message_origin": group_id,
                "forward_to": sorted(list(set(user_sids)))
            })
        self.config["message_forward"] = message_forward
        self.config.save_config()
        current_users = self.get_forward_to_list(group_id)
        yield event.plain_result(
            f"[防撤回插件] 成功更新群组 {group_id} 的防撤回任务。\n当前接收用户: {','.join(current_users)}"
        )

    @anti_recall.command("删除", alias={'移除', 'remove', 'rm', 'delete', 'del'})
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def remove_anti_recall_task(self, event: AstrMessageEvent, group_id: str, user_list: str):
        user_ids_to_remove = {user.strip() for user in user_list.split(',')}
        message_forward = self.config.get("message_forward", [])
        task_found = False
        for task in message_forward:
            if task.get("message_origin") == group_id:
                task_found = True
                current_users = set(task.get("forward_to", []))
                current_users -= user_ids_to_remove
                if not current_users:
                    message_forward.remove(task)
                else:
                    task["forward_to"] = sorted(list(current_users))
                break
        if not task_found:
            yield event.plain_result(f"[防撤回插件] 未找到群组 {group_id} 的防撤回任务。")
            return
        
        self.config["message_forward"] = message_forward
        self.config.save_config()
        
        remaining_users = self.get_forward_to_list(group_id)
        if not remaining_users:
            yield event.plain_result(f"[防撤回插件] 已从群组 {group_id} 中移除指定用户，该群组监控任务已删除。")
        else:
            yield event.plain_result(
                f"[防撤回插件] 已从群组 {group_id} 中移除指定用户。\n剩余接收用户: {','.join(remaining_users)}"
            )

    @anti_recall.command("查看", alias={'list', 'show', 'ls'})
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def list_anti_recall_tasks(self, event: AstrMessageEvent):
        message_forward = self.config.get("message_forward", [])
        if not message_forward:
            yield event.plain_result("[防撤回插件] 当前没有任何防撤回任务。")
            return

        result = "[防撤回插件] 当前防撤回任务列表:\n"
        for i, task in enumerate(message_forward):
            group_id = task.get("message_origin")
            forward_to = task.get("forward_to", [])
            result += f"任务{i+1}: 群组ID: {group_id}\n  接收用户: {','.join(forward_to)}\n"
        yield event.plain_result(result.strip())