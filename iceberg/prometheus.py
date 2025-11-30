from google.adk.plugins.base_plugin import BasePlugin
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_response import LlmResponse
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
import logging

class PrometheusPlugin(BasePlugin):
    """Plugin that pushes LLM metrics to Prometheus push gateway."""

    def __init__(self, pushgateway_url: str, job_name: str = "adk_job"):
        super().__init__(name="prometheus_plugin")
        self.pushgateway_url = pushgateway_url
        self.job_name = job_name

        # Create a registry for this plugin
        self.registry = CollectorRegistry()

        # Define metrics with additional labels
        self.llm_calls_total = Counter(
            'adk_llm_calls_total',
            'Total number of LLM calls',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.input_tokens_total = Counter(
            'adk_input_tokens_total',
            'Total number of input tokens',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.last_input_tokens = Gauge(
            'adk_last_input_tokens',
            'Input tokens from last LLM call',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.output_tokens_total = Counter(
            'adk_output_tokens_total',
            'Total number of output tokens',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.last_output_tokens = Gauge(
            'adk_last_output_tokens',
            'Output tokens from last LLM call',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.thoughts_tokens_total = Counter(
            'adk_thoughts_tokens_total',
            'Total number of thoughts tokens',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.last_thoughts_tokens = Gauge(
            'adk_last_thoughts_tokens',
            'Thoughts tokens from last LLM call',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.cached_tokens_total = Counter(
            'adk_cached_tokens_total',
            'Total number of cached tokens',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.last_cached_tokens = Gauge(
            'adk_last_cached_tokens',
            'Cached tokens from last LLM call',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

        self.approx_cost_total = Gauge(
            'adk_approx_cost_total',
            'Total approx cost',
            ['agent_name', 'model', 'session_id', 'user_id', 'app_name'],
            registry=self.registry
        )

    async def after_model_callback(
            self,
            *,
            callback_context: CallbackContext,
            llm_response: LlmResponse
    ) -> None:
        """Push metrics after each LLM call."""

        # Get agent and session information
        agent_name = callback_context.agent_name
        model = llm_response.model_version or "unknown"

        # Access session details through callback_context.session
        session_id = callback_context.session.id
        user_id = callback_context.user_id  # Also available directly
        app_name = callback_context.session.app_name

        # Extract token usage from response
        if llm_response.usage_metadata:
            input_tokens = llm_response.usage_metadata.prompt_token_count or 0
            output_tokens = llm_response.usage_metadata.candidates_token_count or 0
            thoughts_tokens = llm_response.usage_metadata.thoughts_token_count or 0
            cached_tokens = llm_response.usage_metadata.cached_content_token_count or 0

            # Update counters with all labels
            self.llm_calls_total.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).inc()

            self.input_tokens_total.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).inc(input_tokens)

            self.last_input_tokens.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).set(input_tokens)

            self.output_tokens_total.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).inc(output_tokens)

            self.last_output_tokens.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).set(output_tokens)

            self.thoughts_tokens_total.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).inc(thoughts_tokens)

            self.last_thoughts_tokens.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).set(thoughts_tokens)

            self.cached_tokens_total.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).inc(cached_tokens)

            self.last_cached_tokens.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).set(cached_tokens)

            # based on https://cloud.google.com/vertex-ai/generative-ai/pricing for Gemini 2.5 flash on 11/30/2025
            self.approx_cost_total.labels(
                agent_name=agent_name,
                model=model,
                session_id=session_id,
                user_id=user_id,
                app_name=app_name
            ).inc(input_tokens / 1_000_000.00 * 0.3 + (output_tokens + thoughts_tokens )/ 1_000_000.00 * 2.5 + cached_tokens / 1_000_000.00 * 0.03)
            try:
                push_to_gateway(
                    self.pushgateway_url,
                    job=self.job_name,
                    registry=self.registry
                )
            except Exception as e:
                logging.error(f"Failed to push metrics to Prometheus: {e}")