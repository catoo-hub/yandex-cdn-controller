# Yandex CDN Controller

Переносимый контроллер Yandex Cloud CDN с двумя режимами: поколения с Cloudflare/Certificate
Manager/Remnawave и пересоздание одного ресурса на месте без обязательного пользовательского домена.

## Безопасность

- По умолчанию включён `DRY_RUN=true`.
- HTTP `451` и provider suspension переводят цель в `ATTENTION` и не запускают ротацию.
- В режиме поколений retired-ресурсы автоматически не удаляются.
- `recreate_in_place` удаляет активный ресурс только после достижения `recreate_at_gib` или
  подтверждённой ручной команды `/recreate`.
- Telegram доступен только разрешённым user ID; опасные команды требуют подтверждения.
- Telegram-контейнер не получает provider credentials и Docker socket.

## Первый запуск

```bash
cp config.example.yml config.yml
cp .env.example .env
chmod 600 .env
nano config.yml
nano .env
docker compose build
docker compose run --rm controller cli validate
docker compose run --rm controller cli reconcile
docker compose up -d
```

Не выключайте `DRY_RUN`, пока не проверены конфигурация, импорт существующего ресурса и staging-ротация.

## Что записывать в config.yml

Для текущей схемы `nyako.app` основа цели уже заполнена в `config.example.yml`. Заменить нужно
только значения `replace-with-*`, числовой ID origin group и адрес панели:

| Параметр | Что это | Где взять |
|---|---|---|
| `providers.remnawave.primary.base_url` | URL панели Remnawave | Например `https://panel.example.com`, без `/` в конце |
| `folder_id` | Каталог Yandex Cloud для сертификатов и CDN | Открыть каталог Yandex Cloud → «Обзор» → «ID каталога» |
| `origin_group_id` | Существующая группа источников CDN | Cloud CDN → «Группы источников» → открыть группу, ведущую на `dewl.nyako.app` |
| `origin_host_header` | Настоящий домен origin-сервера | Для нашей ноды: `dewl.nyako.app` |
| `cloudflare_zone_id` | ID зоны `nyako.app` | Cloudflare → `nyako.app` → Overview → Zone ID |
| `host_id` | UUID редактируемого Remnawave Host | Панель Remnawave → Hosts; это не Node UUID |
| `pattern` | Шаблон новых CDN-доменов | `yc-de-{sequence:03d}.nyako.app` создаст `yc-de-001...`, `002...` |
| `path` | XHTTP path | Должен совпадать с inbound и ссылкой: `/content/sec.mp4/` |

`origin_group_id` нельзя заменить ID существующего CDN-ресурса. Контроллер создаёт новые
CDN-ресурсы, но подключает каждый из них к одной существующей origin group.

Для Remnawave Panel `2.7.3–2.7.4` используется официальный contract `2.7.2`:
`GET /api/hosts/{uuid}` и `PATCH /api/hosts` с `uuid` в body. Этот минимальный payload также
совместим с Panel `2.8.x`; новые дополнительные nullable-поля не отправляются. Перед live-запуском
`cli validate` выполняет только read-only GET нужного Host, проверяет UUID и читает одну DNS-запись
Cloudflare. Отдельное право `Zone:Read` не требуется: достаточно zone-scoped `DNS:Edit`.

## Что записывать в .env

| Переменная | Значение |
|---|---|
| `YANDEX_AUTHORIZED_KEY_FILE` | Путь внутри контейнера: `/run/secrets/yandex-authorized-key.json` |
| `CLOUDFLARE_API_TOKEN` | Token с правом `Zone / DNS / Edit` только для `nyako.app` |
| `REMNAWAVE_PRIMARY_TOKEN` | API token панели Remnawave |
| `CONTROLLER_TOKEN` | Случайная строка из `openssl rand -hex 32` |
| `TELEGRAM_BOT_TOKEN` | Токен, выданный `@BotFather` |
| `TELEGRAM_ALLOWED_USER_IDS` | Кто может выполнять команды, Telegram user ID через запятую |
| `TELEGRAM_CHAT_IDS` | Куда отправлять уведомления, chat ID через запятую |
| `TELEGRAM_TOPIC_MAP` | Необязательные пары `chat_id:message_thread_id` для уведомлений в топики |

`CONFIG_PATH` и `DATABASE_PATH` при Docker Compose менять не требуется. Пока `DRY_RUN=true`,
контроллер не изменяет Yandex, Cloudflare и Remnawave.

### Авторизованный ключ Yandex на сервере

Cloud CDN и Certificate Manager не принимают обычный `Api-Key`. Контроллер использует JSON
авторизованного RSA-ключа для выпуска JWT и автоматически обменивает его на IAM token.

В каталоге клонированного репозитория на сервере выполните:

```bash
mkdir -p secrets
chmod 700 secrets
install -o 10001 -g 10001 -m 600 \
  /путь/к/authorized_key.json \
  secrets/yandex-authorized-key.json
```

Файл монтируется Compose в controller как read-only. Он исключён через `.gitignore`; Telegram-контейнер
его не получает. Сервисному аккаунту в каталоге нужны роли `cdn.editor`,
`certificate-manager.editor` и `monitoring.viewer`.

Проверка владельца и режима на сервере:

```bash
stat -c '%a %u:%g %n' secrets/yandex-authorized-key.json
```

Ожидается `600 10001:10001`. При `DRY_RUN=false` команда `cdnctl validate` подписывает JWT и
проверяет получение IAM token, но не создаёт и не изменяет облачные ресурсы.

## Импорт существующего CDN

```bash
docker compose run --rm controller cli import-existing \
  de-main RESOURCE_ID existing-cdn.example.com --bytes-sent 0
```

Yandex хранит доступные для чтения метрики ограниченное время, поэтому при импорте рекомендуется
передать текущий накопленный lifetime total в байтах через `--bytes-sent`.

## Пересоздание одного CDN на месте

Этот режим подходит, когда служебный `providerCname` Yandex закреплён за аккаунтом и после
пересоздания остаётся тем же. Новые домены, сертификаты, Cloudflare-записи и изменение Remnawave
не выполняются.

```yaml
targets:
  - id: direct-yandex
    enabled: true
    yandex:
      folder_id: replace-with-yandex-folder-id
      origin_group_id: 123456
      origin_protocol: HTTP
      origin_host_header: 203.0.113.10
    transport:
      port: 443
      path: /api/uploadFile/
      expected_root_status: 200
      expected_path_status: 400
      healthcheck_mode: provider_only
    rotation:
      mode: recreate_in_place
      recreate_at_gib: 740
```

Если клиенты используют пользовательский домен, добавьте только:

```yaml
domain:
  name: cdn.example.com
```

Если `domain` отсутствует, endpoint для health-check берётся из `providerCname` Yandex. При
пересоздании контроллер читает текущий ресурс, сохраняет его основной `cname` и сертификат (если
он есть), удаляет ресурс, создаёт его с теми же параметрами и сбрасывает локальный счётчик после
успешного health-check. Если новый `providerCname` отличается от прежнего, цель переводится в
`ATTENTION`: DNS автоматически не меняется.

`healthcheck_mode: provider_only` предназначен для CDN, доступных только из мобильных сетей.
Контроллер не делает HTTP-запросов со своего серверного IP, а проверяет через Yandex API, что
Resource активен, имеет `providerCname`, сертификат готов и статус не равен `BLOCKED`,
`SUSPENDED`, `FAILED` или `ERROR`. В этом режиме `expected_root_status` и
`expected_path_status` не используются.

Первичный импорт для прямого служебного домена:

```bash
docker compose run --rm controller cli import-existing \
  direct-yandex RESOURCE_ID account-name.topology.gslb.yccdn.ru --bytes-sent 0
```

Ручной запуск является разрушительной операцией и требует подтверждения в Telegram:

```bash
docker compose run --rm -e DRY_RUN=false controller cli recreate direct-yandex
```

В `.env` для такой цели достаточно Yandex credentials; Cloudflare и Remnawave tokens могут быть
пустыми. Роль `certificate-manager.editor` нужна только тогда, когда используется режим поколений
или требуется создавать новые сертификаты.

## Переключение в live

1. Создайте Yandex service account с ролями CDN editor, Certificate Manager editor и Monitoring viewer.
2. Создайте Cloudflare token с DNS Write только для нужной зоны.
3. Создайте отдельный Remnawave token и укажите точный Host UUID.
4. Проверьте `validate`, `status` и dry-run `prepare` на staging-цели.
5. Установите `DRY_RUN=false`, пересоздайте контейнеры и сначала выполните явный `prepare`.

## HTTP и мониторинг

- `GET /healthz` — liveness.
- `GET /readyz` — готовность scheduler и SQLite.
- `GET /metrics` — Prometheus/OpenMetrics.
- `GET /api/v1/status` — JSON с Bearer-аутентификацией.

Compose публикует порт только на `127.0.0.1:9187`. Для удалённого мониторинга используйте закрытую
сеть либо reverse proxy с аутентификацией. Поддержаны Prometheus, Uptime Kuma Push, подписанный
generic webhook и JSON logs в stdout.

## Telegram

Заполните `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ALLOWED_USER_IDS` и `TELEGRAM_CHAT_IDS`. Бот использует
long polling. Есть `/status`, `/traffic`, `/prepare`, `/rotate`, `/recreate`, `/rollback`, `/pause`, `/resume` и
`/recheck`. `/cleanup` намеренно показывает preview; необратимое удаление остаётся за оператором.

### Telegram Forum Topics

Добавьте бота в группу с включёнными темами, откройте нужный топик и отправьте `/whereami`. Бот
вернёт `chat_id` и `message_thread_id`. Для автоматических уведомлений заполните:

```dotenv
TELEGRAM_CHAT_IDS=-1001234567890
TELEGRAM_TOPIC_MAP=-1001234567890:42
```

Для нескольких групп/топиков пары разделяются запятыми. Команды, отправленные непосредственно в
топике, получают ответ в том же топике автоматически. Если для `chat_id` нет записи в
`TELEGRAM_TOPIC_MAP`, уведомление отправляется в основной чат.

## Проверка контрактов API

Формы envelope в Yandex API и payload Remnawave Host могут отличаться между версиями. Перед live
режимом сохраните read-only ответы Certificate Manager, CDN Resource и Remnawave Host как fixtures
и прогоните contract tests. Любая неизвестная форма ответа приводит к fail-closed, а не к смене
состояния поколения.
