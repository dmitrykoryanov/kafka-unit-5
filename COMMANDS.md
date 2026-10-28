# Команды: сертификаты, хранилища и права доступа

Здесь собраны все команды, которыми создаются сертификаты, хранилища ключей и права доступа (ACL) кластера. Описание проекта и порядок проверки задания — в [README.md](README.md).

**Как пользоваться**

- Команды выполняются в bash (Linux, macOS, WSL, Git Bash) из корня проекта.
- Порядок важен: сначала CA, затем брокеры и клиенты (их сертификаты подписывает CA), в конце — ACL (для них нужен запущенный кластер).
- Во всех хранилищах один пароль — `your-password`.
- Сертификаты уже выпущены и лежат в проекте, команды нужны, чтобы выпустить их заново. Перед этим удалите старые хранилища (`*.jks`, `*.p12`, `*.pkcs12`), иначе `keytool -import` завершится ошибкой: запись `ca` уже существует. После перевыпуска перезапустите брокеры: `docker compose restart kafka1 kafka2 kafka3`.

## Содержание

1. [Корневой сертификат (CA)](#1-корневой-сертификат-ca)
2. [Сертификаты и хранилища брокеров](#2-сертификаты-и-хранилища-брокеров)
3. [Клиентские сертификаты](#3-клиентские-сертификаты)
4. [Права доступа (ACL)](#4-права-доступа-acl)

## 1. Корневой сертификат (CA)

Собственный удостоверяющий центр. Он подписывает сертификаты брокеров и клиентов, и все участники доверяют только ему. Параметры (имя `yandex-practice-kafka-ca`, организация) заданы в `ca.cnf`.

```bash
# Закрытый ключ CA (ca.key) и самоподписанный сертификат CA (ca.crt) сроком на 10 лет
openssl req -new -nodes \
-x509 \
-days 3650 \
-newkey rsa:2048 \
-keyout ca.key \
-out ca.crt \
-config ca.cnf

# Сертификат и ключ CA в одном файле: из него openssl pkcs12 -export -chain берёт цепочку
cat ca.crt ca.key > ca.pem
```

| Файл | Что это |
|---|---|
| `ca.key` | закрытый ключ CA, им подписываются все сертификаты |
| `ca.crt` | сертификат CA, его кладут в truststore всех участников |
| `ca.pem` | `ca.crt` и `ca.key` в одном файле |
| `ca.srl` | счётчик серийных номеров, создаётся при первой подписи (`-CAcreateserial`) |

## 2. Сертификаты и хранилища брокеров

Для каждого брокера `kafkaN` (N = 1, 2, 3) выполняется одна и та же последовательность команд. Параметры сертификата заданы в `kafkaN-creds/kafkaN.cnf`: имя (CN) `kafkaN` и альтернативные имена (SAN) `kafkaN`, `kafkaN-external`, `localhost`, по которым клиенты и другие брокеры проверяют сервер. Сертификат годится и для сервера (`serverAuth`), и для клиента (`clientAuth`): при обмене между брокерами каждый из них выступает клиентом.

| Файл в `kafkaN-creds` | Назначение | Где используется |
|---|---|---|
| `kafkaN.key`, `kafkaN.csr`, `kafkaN.crt` | закрытый ключ, запрос на подпись, подписанный сертификат | промежуточные файлы |
| `kafkaN.p12` | ключ, сертификат и цепочка CA в формате PKCS12 | промежуточный файл |
| `kafkaN.keystore.jks` | keystore брокера | `KAFKA_SSL_KEYSTORE_FILENAME` |
| `kafka.kafkaN.truststore.jks` | truststore брокера с сертификатом CA | `KAFKA_SSL_TRUSTSTORE_FILENAME` |
| `kafkaN_sslkey_creds`, `kafkaN_keystore_creds`, `kafkaN_truststore_creds` | пароли ключа, keystore и truststore | `KAFKA_SSL_*_CREDENTIALS` |
| `kafka.kafkaN.keystore.pkcs12`, `kafkaN.truststore.jks` | keystore в формате PKCS12 и копия truststore | текущей конфигурацией не используются |

Образ `cp-kafka` читает пароли из файлов `*_creds`. Они должны заканчиваться переводом строки LF без `\r`: `echo` в bash пишет именно так.

### kafka1

```bash
# 1. Закрытый ключ брокера (kafka1.key) и запрос на подпись сертификата (kafka1.csr)
openssl req -new \
-newkey rsa:2048 \
-keyout kafka1-creds/kafka1.key \
-out kafka1-creds/kafka1.csr \
-config kafka1-creds/kafka1.cnf \
-nodes

# 2. CA подписывает запрос: сертификат kafka1.crt на 10 лет с расширениями из секции v3_req (SAN, serverAuth, clientAuth)
openssl x509 -req \
-days 3650 \
-in kafka1-creds/kafka1.csr \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-out kafka1-creds/kafka1.crt \
-extfile kafka1-creds/kafka1.cnf \
-extensions v3_req

# 3. Ключ, сертификат и цепочка CA в одном контейнере PKCS12 (kafka1.p12)
openssl pkcs12 -export \
-in kafka1-creds/kafka1.crt \
-inkey kafka1-creds/kafka1.key \
-chain \
-CAfile ca.pem \
-name kafka1 \
-out kafka1-creds/kafka1.p12 \
-password pass:your-password

# 4. Keystore в формате PKCS12 (текущей конфигурацией не используется)
keytool -importkeystore \
-deststorepass your-password \
-destkeystore kafka1-creds/kafka.kafka1.keystore.pkcs12 \
-srckeystore kafka1-creds/kafka1.p12 \
-deststoretype PKCS12  \
-srcstoretype PKCS12 \
-noprompt \
-srcstorepass your-password

# 5. Truststore брокера с сертификатом CA (KAFKA_SSL_TRUSTSTORE_FILENAME)
keytool -import \
-file ca.crt \
-alias ca \
-keystore kafka1-creds/kafka.kafka1.truststore.jks \
-storepass your-password \
-noprompt

# 6. Пароли ключа, keystore и truststore для образа cp-kafka (KAFKA_SSL_*_CREDENTIALS)
echo "your-password" > kafka1-creds/kafka1_sslkey_creds
echo "your-password" > kafka1-creds/kafka1_keystore_creds
echo "your-password" > kafka1-creds/kafka1_truststore_creds

# 7. Keystore брокера в формате JKS (KAFKA_SSL_KEYSTORE_FILENAME)
keytool -importkeystore \
-srckeystore kafka1-creds/kafka1.p12 \
-srcstoretype PKCS12 \
-srcstorepass your-password \
-destkeystore kafka1-creds/kafka1.keystore.jks \
-deststoretype JKS \
-deststorepass your-password

# 8. Копия truststore с сертификатом CA (текущей конфигурацией не используется)
keytool -import -trustcacerts -file ca.crt \
-keystore kafka1-creds/kafka1.truststore.jks \
-storepass your-password -noprompt -alias ca
```

### kafka2

Те же шаги, что для kafka1.

```bash
# 1. Закрытый ключ и запрос на подпись
openssl req -new \
-newkey rsa:2048 \
-keyout kafka2-creds/kafka2.key \
-out kafka2-creds/kafka2.csr \
-config kafka2-creds/kafka2.cnf \
-nodes

# 2. Подпись сертификата CA
openssl x509 -req \
-days 3650 \
-in kafka2-creds/kafka2.csr \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-out kafka2-creds/kafka2.crt \
-extfile kafka2-creds/kafka2.cnf \
-extensions v3_req

# 3. Ключ, сертификат и цепочка CA в PKCS12
openssl pkcs12 -export \
-in kafka2-creds/kafka2.crt \
-inkey kafka2-creds/kafka2.key \
-chain \
-CAfile ca.pem \
-name kafka2 \
-out kafka2-creds/kafka2.p12 \
-password pass:your-password

# 4. Keystore в формате PKCS12 (не используется)
keytool -importkeystore \
-deststorepass your-password \
-destkeystore kafka2-creds/kafka.kafka2.keystore.pkcs12 \
-srckeystore kafka2-creds/kafka2.p12 \
-deststoretype PKCS12  \
-srcstoretype PKCS12 \
-noprompt \
-srcstorepass your-password

# 5. Truststore брокера
keytool -import \
-file ca.crt \
-alias ca \
-keystore kafka2-creds/kafka.kafka2.truststore.jks \
-storepass your-password \
-noprompt

# 6. Пароли для образа cp-kafka
echo "your-password" > kafka2-creds/kafka2_sslkey_creds
echo "your-password" > kafka2-creds/kafka2_keystore_creds
echo "your-password" > kafka2-creds/kafka2_truststore_creds

# 7. Keystore брокера в формате JKS
keytool -importkeystore \
-srckeystore kafka2-creds/kafka2.p12 \
-srcstoretype PKCS12 \
-srcstorepass your-password \
-destkeystore kafka2-creds/kafka2.keystore.jks \
-deststoretype JKS \
-deststorepass your-password

# 8. Копия truststore (не используется)
keytool -import -trustcacerts -file ca.crt \
-keystore kafka2-creds/kafka2.truststore.jks \
-storepass your-password -noprompt -alias ca
```

### kafka3

Те же шаги, что для kafka1.

```bash
# 1. Закрытый ключ и запрос на подпись
openssl req -new \
-newkey rsa:2048 \
-keyout kafka3-creds/kafka3.key \
-out kafka3-creds/kafka3.csr \
-config kafka3-creds/kafka3.cnf \
-nodes

# 2. Подпись сертификата CA
openssl x509 -req \
-days 3650 \
-in kafka3-creds/kafka3.csr \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-out kafka3-creds/kafka3.crt \
-extfile kafka3-creds/kafka3.cnf \
-extensions v3_req

# 3. Ключ, сертификат и цепочка CA в PKCS12
openssl pkcs12 -export \
-in kafka3-creds/kafka3.crt \
-inkey kafka3-creds/kafka3.key \
-chain \
-CAfile ca.pem \
-name kafka3 \
-out kafka3-creds/kafka3.p12 \
-password pass:your-password

# 4. Keystore в формате PKCS12 (не используется)
keytool -importkeystore \
-deststorepass your-password \
-destkeystore kafka3-creds/kafka.kafka3.keystore.pkcs12 \
-srckeystore kafka3-creds/kafka3.p12 \
-deststoretype PKCS12  \
-srcstoretype PKCS12 \
-noprompt \
-srcstorepass your-password

# 5. Truststore брокера
keytool -import \
-file ca.crt \
-alias ca \
-keystore kafka3-creds/kafka.kafka3.truststore.jks \
-storepass your-password \
-noprompt

# 6. Пароли для образа cp-kafka
echo "your-password" > kafka3-creds/kafka3_sslkey_creds
echo "your-password" > kafka3-creds/kafka3_keystore_creds
echo "your-password" > kafka3-creds/kafka3_truststore_creds

# 7. Keystore брокера в формате JKS
keytool -importkeystore \
-srckeystore kafka3-creds/kafka3.p12 \
-srcstoretype PKCS12 \
-srcstorepass your-password \
-destkeystore kafka3-creds/kafka3.keystore.jks \
-deststoretype JKS \
-deststorepass your-password

# 8. Копия truststore (не используется)
keytool -import -trustcacerts -file ca.crt \
-keystore kafka3-creds/kafka3.truststore.jks \
-storepass your-password -noprompt -alias ca
```

## 3. Клиентские сертификаты

Сертификаты администратора и клиентов приложения. Брокер берёт имя пользователя из CN сертификата: `CN=producer` → `User:producer` (правило `KAFKA_SSL_PRINCIPAL_MAPPING_RULES` в `docker-compose.yml`). Сертификаты нужны только для аутентификации клиента (`clientAuth`), поэтому SAN в них нет. Параметры заданы в `<имя>-creds/<имя>.cnf`.

| Клиент | Пользователь Kafka | Кто подключается |
|---|---|---|
| `admin` | `User:admin`, суперпользователь | приложение при создании топиков; утилиты `kafka-topics` и `kafka-acls` внутри контейнеров брокеров |
| `producer` | `User:producer` | продюсер приложения |
| `consumer` | `User:consumer` | консьюмеры приложения |

В каждой папке `<имя>-creds` приложение использует `<имя>.keystore.jks` и `<имя>.truststore.jks` (см. `application.properties`). Папка `admin-creds` дополнительно монтируется в брокеры как `/etc/kafka/admin`.

### admin

```bash
# 1. Закрытый ключ и запрос на подпись
openssl req -new \
-newkey rsa:2048 \
-keyout admin-creds/admin.key \
-out admin-creds/admin.csr \
-config admin-creds/admin.cnf \
-nodes

# 2. CA подписывает клиентский сертификат на 10 лет (только clientAuth)
openssl x509 -req \
-days 3650 \
-in admin-creds/admin.csr \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-out admin-creds/admin.crt \
-extfile admin-creds/admin.cnf \
-extensions v3_req

# 3. Ключ, сертификат и цепочка CA в PKCS12
openssl pkcs12 -export \
-in admin-creds/admin.crt \
-inkey admin-creds/admin.key \
-chain \
-CAfile ca.pem \
-name admin \
-out admin-creds/admin.p12 \
-password pass:your-password

# 4. Keystore клиента (JKS)
keytool -importkeystore \
-srckeystore admin-creds/admin.p12 \
-srcstoretype PKCS12 \
-srcstorepass your-password \
-destkeystore admin-creds/admin.keystore.jks \
-deststoretype JKS \
-deststorepass your-password

# 5. Truststore клиента с сертификатом CA
keytool -import -trustcacerts -file ca.crt \
-keystore admin-creds/admin.truststore.jks \
-storepass your-password -noprompt -alias ca

# 6. Настройки подключения для утилит Kafka (kafka-topics, kafka-acls) от имени admin.
#    Пути указаны внутри контейнера брокера, куда admin-creds монтируется как /etc/kafka/admin
echo "security.protocol=SSL" > admin-creds/admin.properties
echo "ssl.keystore.location=/etc/kafka/admin/admin.keystore.jks" >> admin-creds/admin.properties
echo "ssl.keystore.password=your-password" >> admin-creds/admin.properties
echo "ssl.key.password=your-password" >> admin-creds/admin.properties
echo "ssl.truststore.location=/etc/kafka/admin/admin.truststore.jks" >> admin-creds/admin.properties
echo "ssl.truststore.password=your-password" >> admin-creds/admin.properties
```

### producer

```bash
# 1. Закрытый ключ и запрос на подпись
openssl req -new \
-newkey rsa:2048 \
-keyout producer-creds/producer.key \
-out producer-creds/producer.csr \
-config producer-creds/producer.cnf \
-nodes

# 2. Подпись клиентского сертификата CA
openssl x509 -req \
-days 3650 \
-in producer-creds/producer.csr \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-out producer-creds/producer.crt \
-extfile producer-creds/producer.cnf \
-extensions v3_req

# 3. Ключ, сертификат и цепочка CA в PKCS12
openssl pkcs12 -export \
-in producer-creds/producer.crt \
-inkey producer-creds/producer.key \
-chain \
-CAfile ca.pem \
-name producer \
-out producer-creds/producer.p12 \
-password pass:your-password

# 4. Keystore клиента (JKS)
keytool -importkeystore \
-srckeystore producer-creds/producer.p12 \
-srcstoretype PKCS12 \
-srcstorepass your-password \
-destkeystore producer-creds/producer.keystore.jks \
-deststoretype JKS \
-deststorepass your-password

# 5. Truststore клиента с сертификатом CA
keytool -import -trustcacerts -file ca.crt \
-keystore producer-creds/producer.truststore.jks \
-storepass your-password -noprompt -alias ca
```

### consumer

```bash
# 1. Закрытый ключ и запрос на подпись
openssl req -new \
-newkey rsa:2048 \
-keyout consumer-creds/consumer.key \
-out consumer-creds/consumer.csr \
-config consumer-creds/consumer.cnf \
-nodes

# 2. Подпись клиентского сертификата CA
openssl x509 -req \
-days 3650 \
-in consumer-creds/consumer.csr \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-out consumer-creds/consumer.crt \
-extfile consumer-creds/consumer.cnf \
-extensions v3_req

# 3. Ключ, сертификат и цепочка CA в PKCS12
openssl pkcs12 -export \
-in consumer-creds/consumer.crt \
-inkey consumer-creds/consumer.key \
-chain \
-CAfile ca.pem \
-name consumer \
-out consumer-creds/consumer.p12 \
-password pass:your-password

# 4. Keystore клиента (JKS)
keytool -importkeystore \
-srckeystore consumer-creds/consumer.p12 \
-srcstoretype PKCS12 \
-srcstorepass your-password \
-destkeystore consumer-creds/consumer.keystore.jks \
-deststoretype JKS \
-deststorepass your-password

# 5. Truststore клиента с сертификатом CA
keytool -import -trustcacerts -file ca.crt \
-keystore consumer-creds/consumer.truststore.jks \
-storepass your-password -noprompt -alias ca
```

## 4. Права доступа (ACL)

Права выдаются утилитой `kafka-acls` внутри контейнера `kafka1` от имени `User:admin`. Кластер должен быть запущен (`docker compose up -d`). Обёртка `bash -c '...'` нужна, чтобы Git Bash не превратил путь `/etc/kafka/admin/...` в путь Windows.

Права хранятся в ZooKeeper и переживают перезапуск кластера; после `docker compose down -v` их нужно выдать заново. Повторная выдача уже существующего права ничего не меняет. Права можно выдавать до создания топиков.

| Пользователь | Ресурс | Права |
|---|---|---|
| `User:producer` | топики `topic-1`, `topic-2` | Write, Describe |
| `User:consumer` | топик `topic-1` | Read, Describe |
| `User:consumer` | группы `topic-1-group`, `topic-2-group` | Read |
| `User:consumer` | топик `topic-2` | Read — запрет (DENY) |

```bash
# Продюсер: запись в оба топика и чтение их метаданных
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --allow-principal User:producer \
--operation Write --operation Describe \
--topic topic-1 --topic topic-2'

# Консьюмер: чтение topic-1
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --allow-principal User:consumer \
--operation Read --operation Describe \
--topic topic-1'

# Консьюмер: доступ к группам консьюмеров приложения.
# Группа topic-2-group выдана специально: так при чтении topic-2 отказ приходит именно по топику
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --allow-principal User:consumer \
--operation Read \
--group topic-1-group --group topic-2-group'

# Консьюмер: явный запрет чтения topic-2
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--add --deny-principal User:consumer \
--operation Read \
--topic topic-2'
```

Проверка — список всех выданных прав:

```bash
docker exec kafka1 bash -c 'kafka-acls --bootstrap-server kafka1:9092 \
--command-config /etc/kafka/admin/admin.properties \
--list'
```
