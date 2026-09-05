# Roslyn-анализаторы AvtoBus (E-17)

Пакет `AvtoBus.Analyzers` подключается автоматически как `analyzers/dotnet/cs`.
Ниже — только реально существующие правила (источник истины — `src/AvtoBus.Analyzers/Rules.cs`).
Зарезервированные номера без реализации здесь не упоминаются.

| Код | Название | Severity | Категория | Что проверяет |
|---|---|---|---|---|
| AVB001 | No handler for command | Error | Routing | Команда отправлена через `Send()`, но хендлер не зарегистрирован |
| AVB002 | Multiple handlers for command | Error | Routing | У команды больше одного хендлера (должен быть ровно один) |
| AVB003 | Event has no subscribers | Warning | Routing | Событие публикуется, но подписчиков нет |
| AVB004 | Publish used for ICommand | Error | Routing | `ICommand` отправлен через `Publish()` — нужен `Send()` (+ code-fix) |
| AVB005 | Send used for IEvent | Error | Routing | `IEvent` отправлен через `Send()` — нужен `Publish()` (+ code-fix) |
| AVB008 | First parameter is not a message contract | Error | Routing | Первый параметр хендлера — не контракт сообщения |
| AVB010 | Mutable contract | Warning | Contracts | У контракта есть сеттеры — используй `init` / `required init` |
| AVB011 | Contract references domain type | Warning | Contracts | Контракт ссылается на доменный тип — контракты должны быть standalone DTO |
| AVB015 | Decimal without currency | Info | Contracts | `decimal` без `[Currency]` — денежным суммам нужна явная валюта |
| AVB017 | TenantId in contract body | Warning | Contracts | `TenantId` в теле контракта — тенант должен быть в `Envelope.TenantId` |
| AVB020 | Large contract | Warning | Contracts | Контракт сериализуется в ~N КБ — для > 64 КБ рассмотри Claim Check |
| AVB022 | God event | Info | Contracts | Событие-бог (*Updated с кучей полей) — разбей на точечные доменные события |
| AVB040 | Saga queries external service | Warning | Sagas | Сага дергает внешний сервис — лучше нести данные в событиях |
| AVB041 | Saga without timeout | Warning | Sagas | Сага шлет команду без таймаута — добавь `RequestTimeout` против зависших саг |
| AVB050 | Cross-aggregate read | Error | EventSourcing | Агрегат грузит другой агрегат — читай из проекции или слушай события |
| AVB060 | Event not past tense | Info | Naming | Событие не в прошедшем времени (`OrderPlaced`, а не `PlaceOrderEvent`) |

Подробности каждого правила — по `helpLinkUri` (`https://avtobus.dev/e/AVBxxx`, заглушка до публикации сайта).
