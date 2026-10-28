# kafka-ssl — SSL и ACL в кластере Kafka

Кластер Kafka из трёх брокеров, защищённый SSL с взаимной аутентификацией (mTLS) и авторизацией по ACL, и Spring Boot приложение, которое создаёт топики, пишет в них и читает из них. Формулировка задания — в [TASK.md](TASK.md).

## Что реализовано

| Пункт задания | Реализация |
|---|---|
| Сертификаты для каждого брокера | Собственный CA (`ca.crt`, срок 10 лет) и сертификаты `kafka1..3`, подписанные им. Команды — [COMMANDS.md](COMMANDS.md) |
| Truststore и Keystore для каждого брокера | `kafkaN-creds/kafkaN.keystore.jks` и `kafkaN-creds/kafka.kafkaN.truststore.jks` |
| Брокеры в режиме SSL | `docker-compose.yml`: все порты брокеров — SSL, клиентский сертификат обязателен (`ssl.client.auth=required`), обмен между брокерами тоже по SSL |
| Топики `topic-1`, `topic-2` | Создаются приложением при старте: 3 раздела, 3 реплики, `min.insync.replicas=2` |
| `topic-1` доступен продюсерам и консьюмерам | ACL: `User:producer` — Write, Describe; `User:consumer` — Read, Describe |
| `topic-2`: продюсеры пишут, консьюмеры не читают | ACL: `User:producer` — Write, Describe; `User:consumer` — явный запрет Read (DENY) |

## Как устроена безопасность

**Порты брокеров.** У каждого брокера два SSL-порта:

| Порт | Адрес | Назначение |
|---|---|---|
| `SSL` | `kafkaN:9092` (только внутри Docker-сети) | обмен между брокерами и утилиты Kafka внутри контейнеров |
| `EXTERNAL` | `localhost:9093` / `9094` / `9095` | подключение клиентов с хоста |

**Кто есть кто.** Клиент подключается со своим сертификатом, подписанным нашим CA. Брокер берёт имя пользователя из CN сертификата (`ssl.principal.mapping.rules`): `CN=producer` → `User:producer`.

| Пользователь | Сертификат | Роль |
|---|---|---|
| `User:kafka1`, `User:kafka2`, `User:kafka3` | `kafkaN-creds` | брокеры, суперпользователи |
| `User:admin` | `admin-creds` | суперпользователь: создаёт топики, выдаёт ACL |
| `User:producer` | `producer-creds` | продюсер приложения |
| `User:consumer` | `consumer-creds` | консьюмер приложения |

**Права (ACL)** выдаются вручную утилитой `kafka-acls` и хранятся в ZooKeeper (`AclAuthorizer`). Всё, что не разрешено явно, запрещено.

| Пользователь | Ресурс | Права |
|---|---|---|
| `User:producer` | топики `topic-1`, `topic-2` | Write, Describe |
| `User:consumer` | топик `topic-1` | Read, Describe |
| `User:consumer` | группы `topic-1-group`, `topic-2-group` | Read |
| `User:consumer` | топик `topic-2` | Read — **DENY** |

Консьюмеру специально выдан доступ к группе `topic-2-group`: так при чтении `topic-2` он получает отказ именно по топику (`TopicAuthorizationException`), а не по группе.

## Приложение

Пакет `com.dmitrykoryanov.kafka.kafkassl`:

| Класс | Что делает |
|---|---|
| `config/KafkaTopicConfig` | описывает `topic-1` и `topic-2`; Spring Kafka создаёт их при старте от имени `User:admin` |
| `producer/MessageProducer` | после старта отправляет по 10 сообщений в каждый топик от имени `User:producer` |
| `producer/SendResultLogger` | логирует результат каждой отправки: успех (раздел, смещение) или ошибку |
| `consumer/MessageConsumer` | два слушателя от имени `User:consumer`: `topic-1` (группа `topic-1-group`) и `topic-2` (группа `topic-2-group`) |
| `consumer/ConsumerEventsLogger` | логирует остановку и сбой запуска консьюмеров, например отказ в доступе к `topic-2` |

Настройки подключения трёх пользователей — в `src/main/resources/application.properties`. Пути к хранилищам указаны относительно корня проекта, поэтому приложение нужно запускать из него.

## Требования

- Docker Desktop (или Docker Engine с `docker compose`).
- JDK 21.
- `openssl` и `keytool` (входит в JDK) — для проверки SSL и для перевыпуска сертификатов.

Команды ниже приведены для bash (Linux, macOS, WSL, Git Bash) и выполняются из корня проекта. В PowerShell вместо `./gradlew` используйте `gradlew.bat`.

## Проверка задания

### Шаг 0 (необязательно). Перевыпуск сертификатов

Сертификаты и хранилища уже лежат в проекте. Чтобы выпустить их заново, выполните разделы 1–3 из [COMMANDS.md](COMMANDS.md) по порядку: CA → брокеры `kafka1..3` → клиенты `admin`, `producer`, `consumer`. Там же описано, зачем нужна каждая команда и какие файлы она создаёт. Перед повторным запуском удалите старые файлы хранилищ (`*.jks`, `*.p12`, `*.pkcs12`), иначе `keytool -import` завершится ошибкой: запись `ca` уже существует. Если кластер уже запущен, после перевыпуска перезапустите брокеры: `docker compose restart kafka1 kafka2 kafka3`.

### Шаг 1. Запуск кластера

```bash
docker compose up -d
docker compose ps
```

Все четыре контейнера (`zookeeper`, `kafka1`, `kafka2`, `kafka3`) должны быть в состоянии `Up`. Убедиться, что брокеры стартовали с SSL:

```bash
docker logs kafka1 2>&1 | grep -E "SSL is enabled|started \(kafka.server.KafkaServer\)"
```

### Шаг 2. Проверка SSL

С клиентским сертификатом подключение проходит:

```bash
openssl s_client -connect localhost:9093 -tls1_3 -CAfile ca.crt \
  -cert producer-creds/producer.crt -key producer-creds/producer.key </dev/null
```

Ожидается `Verify return code: 0 (ok)` и отсутствие ошибок в конце вывода. Те же команды работают для портов `9094` и `9095`.

Без клиентского сертификата брокер разрывает соединение:

```bash
openssl s_client -connect localhost:9093 -tls1_3 -CAfile ca.crt </dev/null
```

Ожидается ошибка `tls alert bad certificate` (`SSL alert number 42`).

### Шаг 3. Выдача прав (ACL)

Команды выполняются внутри контейнера `kafka1` от имени `User:admin`. Обёртка `bash -c '...'` нужна, чтобы Git Bash не превращал путь `/etc/kafka/admin/...` в путь Windows.

```bash
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --allow-principal User:producer \
--operation Write --operation Describe \
--topic topic-1 --topic topic-2'

docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --allow-principal User:consumer \
--operation Read --operation Describe \
--topic topic-1'

docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --allow-principal User:consumer \
--operation Read \
--group topic-1-group --group topic-2-group'

docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --deny-principal User:consumer \
--operation Read \
--topic topic-2'
```

ACL можно выдавать до создания топиков. Повторная выдача уже существующего правила ничего не меняет. Права сохраняются в томе ZooKeeper и переживают `docker compose down`; после `docker compose down -v` их нужно выдать заново.

Проверка:

```bash
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties --list'
```

Ожидаемый вывод (порядок строк может отличаться):

```
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=topic-1, patternType=LITERAL)`:
 	(principal=User:producer, host=*, operation=WRITE, permissionType=ALLOW)
	(principal=User:producer, host=*, operation=DESCRIBE, permissionType=ALLOW)
	(principal=User:consumer, host=*, operation=DESCRIBE, permissionType=ALLOW)
	(principal=User:consumer, host=*, operation=READ, permissionType=ALLOW)

Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=topic-2, patternType=LITERAL)`:
 	(principal=User:producer, host=*, operation=DESCRIBE, permissionType=ALLOW)
	(principal=User:consumer, host=*, operation=READ, permissionType=DENY)
	(principal=User:producer, host=*, operation=WRITE, permissionType=ALLOW)

Current ACLs for resource `ResourcePattern(resourceType=GROUP, name=topic-1-group, patternType=LITERAL)`:
 	(principal=User:consumer, host=*, operation=READ, permissionType=ALLOW)

Current ACLs for resource `ResourcePattern(resourceType=GROUP, name=topic-2-group, patternType=LITERAL)`:
 	(principal=User:consumer, host=*, operation=READ, permissionType=ALLOW)
```

### Шаг 4. Запуск приложения

Из корня проекта:

```bash
./gradlew bootRun
```

Либо в IntelliJ IDEA запустите класс `KafkaSslApplication` (рабочий каталог — корень проекта, это значение по умолчанию). Либо соберите и запустите jar:

```bash
./gradlew bootJar
java -jar build/libs/kafka-ssl-0.0.1-SNAPSHOT.jar
```

При старте приложение создаёт `topic-1` и `topic-2`, запускает консьюмеры и отправляет по 10 сообщений в каждый топик.

### Шаг 5. Что должно быть в логах

| Проверка | Что искать в логе |
|---|---|
| Продюсер пишет в оба топика | 20 строк `Sending message` и 20 строк `Message sent` (по 10 на топик), ни одной `Error while producing` |
| Консьюмер читает `topic-1` | `topic-1-group: partitions assigned: [topic-1-0, topic-1-1, topic-1-2]` и 10 строк `Received message: topic=topic-1` |
| Консьюмер не может читать `topic-2` | `TopicAuthorizationException: Not authorized to access topics: [topic-2]` и `Consumer stopped abnormally: listener=topic-2-listener-0, reason=AUTH`; ни одной строки `Received message: topic=topic-2` |

Примеры строк:

```
INFO  ... MessageProducer       : Sending message: topic=topic-1, key=key-1, value=Message 1 to topic-1
INFO  ... SendResultLogger      : Message sent: topic=topic-2, partition=2, offset=0, key=key-2, value=Message 2 to topic-2
INFO  ... KafkaMessageListenerContainer : topic-1-group: partitions assigned: [topic-1-0, topic-1-1, topic-1-2]
INFO  ... MessageConsumer       : Received message: topic=topic-1, partition=2, offset=0, key=key-2, value=Message 2 to topic-1
ERROR ... KafkaMessageListenerContainer : Authentication/Authorization Exception and no authExceptionRetryInterval set
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [topic-2]
ERROR ... ConsumerEventsLogger  : Consumer stopped abnormally: listener=topic-2-listener-0, reason=AUTH
```

Ошибки по `topic-2` — ожидаемый результат: так ACL запрещает консьюмеру чтение.

Приложение не завершается само: консьюмер `topic-1` продолжает ждать новые сообщения. Остановка — `Ctrl+C` или кнопка Stop в IDE. При повторном запуске продюсер снова отправит по 10 сообщений, а консьюмер прочитает только новые: свою позицию в `topic-1` он сохраняет.

### Шаг 6. Проверка топиков

```bash
docker exec kafka1 bash -c 'kafka-topics --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties --describe --topic topic-1'
```

Ожидается `PartitionCount: 3`, `ReplicationFactor: 3`, `Configs: min.insync.replicas=2`, и в каждом разделе в `Isr` все три брокера — значит, репликация между брокерами по SSL работает. Для `topic-2` — то же самое (`kafka-topics --describe` принимает только один `--topic`).

### Остановка кластера

```bash
docker compose down      # данные и ACL сохраняются в томах
docker compose down -v   # удалить всё, включая топики и ACL
```

## Возможные проблемы

| Симптом | Причина и решение |
|---|---|
| `NoSuchFileException ... admin-creds/admin.keystore.jks` при старте приложения | приложение запущено не из корня проекта; исправьте рабочий каталог |
| `NoSuchFileException` с путём вида `C:/Program Files/Git/etc/kafka/admin/...` | Git Bash преобразовал путь; запускайте утилиты Kafka через `bash -c '...'`, как в шагах выше |
| `TopicAuthorizationException` для `topic-1` или ошибки отправки у продюсера | ACL не выданы, например после `docker compose down -v`; выполните шаг 3 |
| `Failed to create new KafkaAdminClient` / таймаут подключения | кластер не запущен или брокеры ещё стартуют; проверьте шаг 1 |

## Замечания

- Пароли (`your-password`) и закрытые ключи лежат в проекте.
- Порт ZooKeeper `2181` открыт на хосте, а ACL хранятся в ZooKeeper. Через него права можно изменить напрямую, в обход авторизации Kafka.
