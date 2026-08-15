from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class RotationState(StrEnum):
    ACTIVE = "ACTIVE"
    PREPARING = "PREPARING"
    READY = "READY"
    SWITCHING = "SWITCHING"
    DRAINING = "DRAINING"
    RETIRED = "RETIRED"
    ATTENTION = "ATTENTION"
    PAUSED = "PAUSED"


class YandexConfig(BaseModel):
    folder_id: str
    origin_group_id: int
    origin_protocol: str = "HTTPS"
    origin_host_header: str

    @field_validator("origin_protocol")
    @classmethod
    def protocol(cls, value: str) -> str:
        value = value.upper()
        if value not in {"HTTP", "HTTPS", "MATCH"}:
            raise ValueError("origin_protocol must be HTTP, HTTPS or MATCH")
        return value


class DomainConfig(BaseModel):
    zone: str
    pattern: str
    cloudflare_zone_id: str

    @model_validator(mode="after")
    def check_pattern(self):
        if "{sequence" not in self.pattern:
            raise ValueError("domain.pattern must contain {sequence}")
        return self

    def render(self, sequence: int) -> str:
        return self.pattern.format(sequence=sequence)


class RemnawaveTarget(BaseModel):
    panel: str
    host_id: str
    update_fields: list[str] = Field(default_factory=lambda: ["address", "sni", "host"])


class TransportConfig(BaseModel):
    port: int = 443
    path: str
    expected_root_status: int = 200
    expected_path_status: int = 400

    @field_validator("path")
    @classmethod
    def path_shape(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("transport.path must start with /")
        return value


class RotationConfig(BaseModel):
    prepare_at_gib: float = 700
    switch_at_gib: float = 800
    technical_failures_before_prepare: int = 3

    @model_validator(mode="after")
    def thresholds(self):
        if self.prepare_at_gib <= 0 or self.switch_at_gib <= self.prepare_at_gib:
            raise ValueError("switch_at_gib must exceed prepare_at_gib")
        return self


class MonitoringTarget(BaseModel):
    uptime_kuma_push_url: str = ""


class TargetConfig(BaseModel):
    id: str
    enabled: bool = True
    yandex: YandexConfig
    domain: DomainConfig
    remnawave: RemnawaveTarget
    transport: TransportConfig
    rotation: RotationConfig = Field(default_factory=RotationConfig)
    monitoring: MonitoringTarget = Field(default_factory=MonitoringTarget)


class RemnawaveProvider(BaseModel):
    base_url: str
    token_env: str
    host_endpoint: str = "/api/hosts/{host_id}"


class ProviderConfig(BaseModel):
    remnawave: dict[str, RemnawaveProvider] = Field(default_factory=dict)


class AppConfig(BaseModel):
    poll_interval_seconds: int = 300
    notification_dedup_seconds: int = 1800
    drain_hours: int = 24
    providers: ProviderConfig = Field(default_factory=ProviderConfig)
    targets: list[TargetConfig]

    @model_validator(mode="after")
    def unique_targets(self):
        ids = [target.id for target in self.targets]
        if len(ids) != len(set(ids)):
            raise ValueError("target ids must be unique")
        missing = sorted({
            target.remnawave.panel for target in self.targets
            if target.remnawave.panel not in self.providers.remnawave
        })
        if missing:
            raise ValueError(f"unknown Remnawave panel(s): {', '.join(missing)}")
        return self

    def target(self, target_id: str) -> TargetConfig:
        for target in self.targets:
            if target.id == target_id:
                return target
        raise KeyError(target_id)


class Generation(BaseModel):
    id: int
    target_id: str
    sequence: int
    fqdn: str
    state: RotationState
    resource_id: str | None = None
    certificate_id: str | None = None
    provider_cname: str | None = None
    dns_record_id: str | None = None
    validation_record_id: str | None = None
    bytes_sent: float = 0
    last_metric_ts: float | None = None
    created_at: float
    activated_at: float | None = None
    drain_after: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        return AppConfig.model_validate(yaml.safe_load(handle))
