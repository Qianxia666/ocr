"""
内容道德审查模块 - 严格遵循中国法律法规的AI审查系统
"""

import aiohttp
import asyncio
import logging
import json
import os
from typing import Dict, Any, Tuple, Optional
from enum import Enum
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ModerationAction(Enum):
    """审查动作枚举"""
    ALLOW = "allow"          # 允许
    BLOCK = "block"          # 完全拦截


class ModerationFailedException(Exception):
    """审查失败异常 - 包含原始OCR内容"""
    def __init__(self, message: str, original_content: str, details: Dict[str, Any]):
        super().__init__(message)
        self.original_content = original_content
        self.details = details


class ModerationConfig:
    """审查配置管理器 - ENV优先 > 运行时配置"""

    def __init__(self):
        # 从环境变量加载默认配置（持久化）
        self.env_config = {
            'enabled': os.getenv('MODERATION_ENABLED', 'true').lower() == 'true',
            'api_base_url': os.getenv('MODERATION_API_BASE_URL', os.getenv('API_BASE_URL', 'https://api.openai.com')),
            'api_key': os.getenv('MODERATION_API_KEY', os.getenv('OPENAI_API_KEY', '')),
            'model': os.getenv('MODERATION_MODEL', 'gpt-4o-mini'),
        }

        # 运行时配置（临时，重启失效）
        self.runtime_config = {}

        logger.info(f"审查配置初始化 - ENV配置: enabled={self.env_config['enabled']}, model={self.env_config['model']}")

    def get_config(self, key: str) -> Any:
        """获取配置值 - 优先使用运行时配置"""
        if key in self.runtime_config:
            return self.runtime_config[key]
        return self.env_config.get(key)

    def update_runtime(self, config: Dict[str, Any]):
        """更新运行时配置（仅存内存，重启失效）"""
        self.runtime_config.update(config)
        logger.info(f"运行时审查配置已更新: {config}")

    def reset_runtime(self):
        """重置运行时配置，恢复ENV默认值"""
        self.runtime_config = {}
        logger.info("运行时审查配置已重置为ENV默认值")

    def get_all_config(self) -> Dict[str, Any]:
        """获取当前生效的所有配置"""
        config = self.env_config.copy()
        config.update(self.runtime_config)
        return config


# 严格的审查提示词 - 遵循中国法律法规
MODERATION_SYSTEM_PROMPT = """
你是一个严格遵循中国法律法规的内容审查专家，请审查以下OCR识别的文本内容，判断是否违规内容：

1.你认为违规的内容
2.成段或整篇出现政治内容

**返回格式（JSON）：**
{
    "is_safe": false,
    "violation_types": ["政治内容", "违法内容"],
    "risk_level": "high",
    "reason": "检测到政治相关内容：[具体内容片段]",
    "evidence": "具体违规文本片段（引用原文前50字）"
}

**注意事项：**
- is_safe: true表示内容安全，false表示包含禁止内容
- violation_types: 违规类型列表（可多个）
- risk_level: 固定为"high"（所有违规内容都是高风险）
- reason: 详细说明违规原因
- evidence: 引用具体违规片段（不超过50字）

请严格按照上述标准审查内容，确保符合中国法律法规要求。
"""


class ContentModerator:
    """内容道德审查器 - 核心审查逻辑"""

    def __init__(self, config: ModerationConfig):
        self.config = config
        logger.info("内容审查器初始化完成")

    async def moderate_content(
        self,
        content: str,
        task_id: str = None,
        user_is_admin: bool = False
    ) -> Tuple[ModerationAction, str, Dict[str, Any]]:
        """
        审查内容

        Args:
            content: 待审查的OCR内容
            task_id: 任务ID（用于日志）
            user_is_admin: 用户是否为管理员

        Returns:
            (审查动作, 处理后内容, 审查详情)

        Raises:
            ModerationFailedException: 审查未通过时抛出异常
        """
        # 1. 检查审查是否启用
        if not self.config.get_config('enabled'):
            logger.info(f"任务 {task_id} - 审查功能已禁用，直接通过")
            return ModerationAction.ALLOW, content, {"skipped": True, "reason": "审查功能已禁用"}

        # 2. 管理员豁免检查
        if user_is_admin:
            logger.info(f"任务 {task_id} - 管理员用户，跳过审查")
            return ModerationAction.ALLOW, content, {"skipped": True, "reason": "管理员豁免"}

        # 3. 执行AI审查
        try:
            logger.info(f"任务 {task_id} - 开始AI道德审查，内容长度: {len(content)}")
            ai_result = await self._ai_moderation(content, task_id)

            # 4. 判断审查结果
            if ai_result.get('is_safe', True):
                logger.info(f"任务 {task_id} - 审查通过")
                return ModerationAction.ALLOW, content, ai_result
            else:
                # 审查未通过 - 抛出异常
                violation_types = ai_result.get('violation_types', [])
                reason = ai_result.get('reason', '内容包含违规信息')
                evidence = ai_result.get('evidence', '')

                logger.warning(
                    f"任务 {task_id} - 审查未通过 | "
                    f"违规类型: {violation_types} | "
                    f"原因: {reason} | "
                    f"证据: {evidence[:50]}"
                )

                # 抛出异常，包含原始内容和详情
                raise ModerationFailedException(
                    message="内容道德审查未通过",
                    original_content=content,
                    details=ai_result
                )

        except ModerationFailedException:
            # 直接向上抛出审查失败异常
            raise

        except Exception as e:
            # AI审查API调用失败 - 降级策略
            logger.error(f"任务 {task_id} - AI审查失败: {e}", exc_info=True)

            # 降级策略：审查失败时直接拦截（安全优先）
            logger.warning(f"任务 {task_id} - 审查服务异常，按违规处理（安全优先）")
            raise ModerationFailedException(
                message="内容道德审查服务异常，按违规处理",
                original_content=content,
                details={
                    "error": str(e),
                    "is_safe": False,
                    "violation_types": ["审查服务异常"],
                    "risk_level": "high",
                    "reason": f"审查服务调用失败: {str(e)}"
                }
            )

    async def _ai_moderation(self, content: str, task_id: str) -> Dict[str, Any]:
        """调用AI审查API"""
        api_base_url = self.config.get_config('api_base_url')
        api_key = self.config.get_config('api_key')
        model = self.config.get_config('model')

        # 限制审查内容长度（降低成本）
        max_length = 4000
        truncated_content = content[:max_length]
        if len(content) > max_length:
            logger.info(f"任务 {task_id} - 审查内容已截断至 {max_length} 字符")

        # 构造审查请求
        user_prompt = f"请审查以下内容:\n\n{truncated_content}"

        timeout = aiohttp.ClientTimeout(total=30)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{api_base_url}/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": MODERATION_SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt}
                        ],
                        "temperature": 0.0,
                        "max_tokens": 500
                    }
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        ai_response = result['choices'][0]['message']['content']

                        # 解析AI返回的JSON
                        moderation_result = self._parse_ai_response(ai_response, task_id)
                        return moderation_result
                    else:
                        error_text = await response.text()
                        raise Exception(f"审查API返回错误 {response.status}: {error_text}")

        except asyncio.TimeoutError:
            raise Exception("审查API调用超时")
        except Exception as e:
            raise Exception(f"审查API调用失败: {str(e)}")

    def _parse_ai_response(self, ai_response: str, task_id: str) -> Dict[str, Any]:
        """解析AI审查响应"""
        try:
            # 尝试直接解析JSON
            result = json.loads(ai_response.strip())

            # 验证必需字段
            if 'is_safe' not in result:
                logger.warning(f"任务 {task_id} - AI响应缺少 is_safe 字段，默认为不安全")
                result['is_safe'] = False

            return result

        except json.JSONDecodeError:
            # 如果AI返回不是标准JSON，尝试提取
            logger.warning(f"任务 {task_id} - AI响应不是有效JSON，尝试提取")

            # 尝试从markdown代码块中提取JSON
            if '```json' in ai_response:
                try:
                    json_start = ai_response.find('```json') + 7
                    json_end = ai_response.find('```', json_start)
                    json_str = ai_response[json_start:json_end].strip()
                    return json.loads(json_str)
                except:
                    pass

            # 无法解析 - 保守处理
            logger.error(f"任务 {task_id} - 无法解析AI审查响应，默认拦截: {ai_response[:200]}")
            return {
                "is_safe": False,
                "violation_types": ["AI响应解析失败"],
                "risk_level": "high",
                "reason": "AI审查响应格式错误，保守拦截",
                "evidence": ai_response[:100]
            }


# 全局审查配置实例
moderation_config = ModerationConfig()


def get_moderation_config() -> ModerationConfig:
    """获取全局审查配置实例"""
    return moderation_config
