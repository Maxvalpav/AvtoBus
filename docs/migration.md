# Миграция между версиями

## 0.1.x → 0.2 (тонкое ядро, breaking для preview)

`Hangfire`, `Mongo`, `Actors`, `Canvas` переехали из `AvtoBus.Core` в пакеты
`AvtoBus.Hangfire`, `AvtoBus.Mongo`, `AvtoBus.Actors`, `AvtoBus.Canvas`.
Пространства имён (`AvtoBus.Hangfire`, …) не менялись — достаточно добавить
`PackageReference` на нужный пакет:

```bash
dotnet add package AvtoBus.Hangfire   # только если использовали BackgroundJob-мост
dotnet add package AvtoBus.Mongo      # только если использовали MongoOutbox
dotnet add package AvtoBus.Actors     # только если наследовали VirtualActor
dotnet add package AvtoBus.Canvas     # только если использовали chain/group/chord
```

Метапакет `AvtoBus` состав не менял (Core + InMemory + JSON).

## Подписи v2 → v3 (0.1.2, breaking для preview)

Исходящие перешли на v3 (подписанная метка времени). Входящие принимают v2/v3,
поэтому порядок обновления: сначала консьюмеры (научатся читать v3), потом продюсеры.
`MaxSignatureAge` (5 мин) + `MaxClockSkew` (1 мин) — проверьте часы и допустимую
задержку очередей: конверт старше окна отклоняется как переигрывание.

## Схема outbox v2 → v3

Миграция партиционных лиз `avtobus_outbox_leases` применяется relay автоматически.
Откат ниже v2 не поддерживается — перед обновлением убедитесь, что outbox пуст
(`avtobus.outbox.pending == 0`) или готовы к повторной доставке (inbox её погасит).

## Общее правило

Минор может добавить новую wire-версию, но обязан читать N-1 минимум два минора.
Полная политика — в [compatibility](compatibility.md).
