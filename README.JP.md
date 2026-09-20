# airflow-timetables-calendar

カレンダー駆動の [Airflow](https://airflow.apache.org/) Timetable と、古典的な国産
ジョブスケジューラの語彙（種別 / 開始日 / 振り替え / 起算 / 猶予日数）で書ける
営業日ルールエンジンです。

このパッケージが必要とされる理由は、「月末営業日に実行する」を cron 式では表現
できないことにあります。そして、地域・取引所・祝日ルールごとに日付計算を手書き
することが、スケジューラを静かに壊す典型的な原因です。そこでカレンダーそのものを
再実装するのではなく、すでにデータを保守しているライブラリに委譲し、その上に
国産エンタープライズジョブスケジューラが長年使ってきたスケジュール語彙を重ねて
います。

```bash
pip install airflow-timetables-calendar

# 取引所カレンダー（東証、NYSE、LSE など）はオプトイン
pip install "airflow-timetables-calendar[exchanges]"
```

英語のドキュメントは [`README.md`](README.md) です。

## クイックスタート

```python
from airflow.sdk import DAG
from airflow_timetables_calendar import CalendarTimetable

with DAG(
    dag_id="daily_report",
    # 21:00 JST、日本の全営業日（月〜金から祝日を除いた日）
    schedule=CalendarTimetable(calendar_id="JP", hour=21),
    ...
):
    ...
```

`CalendarTimetable` にルールは必須ではありません。ルールを渡さない場合は
「そのカレンダーの全営業日、指定したローカル時刻に実行」として振る舞います。

## カレンダー

プレフィックスなしの ID は両方のレジストリに対して解決されるため、国コードも
取引所コードも同じ書き方で使えます。

```python
CalendarTimetable(calendar_id="JP", hour=9)             # 日本
CalendarTimetable(calendar_id="US", hour=9)             # アメリカ
CalendarTimetable(calendar_id="US-CA", hour=9)          # カリフォルニア州
CalendarTimetable(calendar_id="DE-BY", hour=9)          # バイエルン州
CalendarTimetable(calendar_id="TSE", hour=9)            # 東京証券取引所
CalendarTimetable(calendar_id="NYSE", hour=9)           # ニューヨーク証券取引所
CalendarTimetable(calendar_id="exchange:XLON", hour=8)  # ロンドン証券取引所
CalendarTimetable(calendar_id="NONE", hour=9)           # 月〜金のみ
```

[`holidays`](https://pypi.org/project/holidays/)（500 以上の国・地域カレンダー）と、
`exchanges` extra を入れた場合は
[`pandas_market_calendars`](https://pypi.org/project/pandas_market_calendars/)
（メンテナンス休場・特別休場を含む 200 以上の取引所）が裏で動いています。

プレフィックスなしのコードは `holidays` → 取引所レジストリの順に解決されます。
同じ市場に対して 2 つのレジストリが別のコードを使うことがあるため、どちらかを
明示したいときはプレフィックスを付けます。

```python
CalendarTimetable(calendar_id="country:JP")      # holidays（国）を強制
CalendarTimetable(calendar_id="exchange:XTKS")   # 東証、mcal
CalendarTimetable(calendar_id="exchange:XLON")   # ロンドン証券取引所、mcal
```

取引所 ID は `pandas_market_calendars` の名前で、一般に略称ではなく MIC コード
です。`TSE` ではなく `XTKS` になります。不明なときは
`available_exchange_calendars()` を確認してください。未知の ID は黙って
スケジュールされるのではなく、DAG パース時に `UnknownCalendarError` を送出します。

それ以外の理由で照会に失敗した場合は、「休日ではない」と答えずに
`ExchangeCalendarError` を送出します。失敗した照会を営業日として扱うと、
カレンダーが保証できない日に実行が入ってしまうためです。本パッケージが避けようと
している「サイレントに誤る」状態そのものです。

## 営業日ルール

`rules` にはプリセット名、`verbose_rule()` / `simple_rule()` の kwargs 辞書、
または `ScheduleRule` オブジェクトを、優先度の低い順に渡します。ルールを渡すと
「全営業日」が**置き換わり**、ルールが生成する日だけが実行されます。

```python
from airflow_timetables_calendar import (
    CalendarTimetable,
    simple_rule,
    verbose_rule,
    nth_business_day,
    nth_business_day_from_end,
    Kind,
)

# プリセット名（英語名・日本語名のどちらでも可）
CalendarTimetable(calendar_id="JP", hour=21, rules=["last_business_day_of_month"])
CalendarTimetable(calendar_id="JP", hour=21, rules=["当月末営業日"])  # 同じルール

# 第10営業日 と 月末営業日
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[nth_business_day(10), nth_business_day_from_end(0)],
)

# 明示形式：引数名が 種別 / 開始日 / 振り替え / 起算 に 1:1 で対応するため、
# 既存の定義をそのまま転記できます。
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[verbose_rule(kind=Kind.ABSOLUTE, day=15, substitution="next", grace_days=3)],
)

# 簡易形式：休止日 / 相対 を通常どおり書く 1 行 1 スケジュールの形式
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[simple_rule(day="L", shift="prev", relative=-2)],  # 月末の3営業日前
)
```

組み込みプリセットは日本語名と英語名の両方を持ち、プリセット名を受け付ける
すべての場所でどちらも使えます。正規キーは日本語名です（元の定義がその語彙で
書かれているため）。自組織の語彙が英語なら英語名を使ってください。どちらの
綴りでも生成される `ScheduleRule` は完全に同一です。

| 英語名 | 日本語名 | 意味 |
|---|---|---|
| `first_business_day_of_month` | `月初営業日` | その月の最初の営業日（第1営業日） |
| `last_business_day_of_month` | `当月末営業日` | その月の最終営業日（月末営業日） |
| `last_business_day_of_previous_month` | `前月末営業日` | **前月**の最終営業日 |
| `business_day_before_month_end` | `月末前営業日` | 月末の前営業日 |
| `next_business_day` | `翌営業日` | 1日の翌営業日（＝1日が営業日なら1日自身） |
| `previous_business_day` | `前営業日` | 1日の前営業日（＝前月末営業日） |
| `every_business_day` | `毎営業日` | すべての営業日 |

```python
CalendarTimetable(calendar_id="JP", hour=21, rules=["last_business_day_of_month"])
```

`PRESET_LOOKUP` は受け付ける全綴りを正規キーへ対応付け、`PRESET_ALIASES` は
英語名だけを保持しています。ツール側で両方の語彙を提示したいときに使えます。
未知の名前は DAG パース時に例外となり、受け付ける全綴りがメッセージに列挙
されます。

### モデル

日付は古典モデルに従い、**アンカー**と一連の**修飾子**から導出されます。

| 概念 | 意味 |
|---|---|
| 基準日 | 「1 か月」の開始位置。`base_day=26` なら 2026-08-26〜2026-09-25 が「8 月」という営業月になる |
| 基準時刻 | 古典モデルが*業務上の日付*を切り替える時刻。08:00〜翌 07:59 を 1 営業日とし、「25:00」の実行を前日分として扱えるようにするもの。**一部実装済み**。`hour` は 48 時間制を受け付けるため、0〜23 以外の実行は隣の暦日にずらしつつ基準日の業務日に属します（[48 時間制](#48-時間制)参照）。基準時刻そのものを任意の時刻に設定する機能は未実装です |
| 種別 | オフセットが何を数えるか。`ABSOLUTE` 暦日、`RELATIVE` 相対日（基準日からの暦日。`base_day=1` では `ABSOLUTE` と同一）、`OPERATING` 運用日（→ 第 n 営業日）、`CLOSED` 休業日、`REGISTERED` 登録日 |
| 開始日 | `DAY` 日付指定、`MONTH_END` 月末指定、`WEEKDAY` 曜日指定。算出された日付が*何を基準に*数えられるかは種別で決まります（[開始日の基準はどこか](#開始日の基準はどこか)参照） |
| 休業日の振り替え | 算出日が休業日のときの扱い。`SKIP` 実行しない、`PREVIOUS` 前の運用日、`NEXT` 次の運用日、`RUN_ANYWAY` 振り替えなし |
| 起算スケジュール | 最後に行う `n` 運用日（`OPERATING`）または暦日（`CALENDAR`）の調整 |
| 処理サイクル | 繰り返し期間。実装済みは `daily`（1日毎）と `monthly`（1月毎）のみで、`weekly` / `yearly` は語彙としては受け付けますが、黙って月次になることを避けるため生成時に拒否します |
| 猶予日数 | 振り替え・起算が移動できる最大距離。**暦日**で数えます。**これを超えるとその回は実行スケジュールが算出されません** — 古典モデルどおり、エラーではなく実行なしです。猶予日数はルールが到達できる範囲も画すため、広げるほど `matches()` の計算量が増えます。**`grace_days=0` は「既定値を使う」の意味で、`0` 日という意味ではありません**。既定値より狭い値は指定できません（`0` は既定値の選択を意味し、それ以外は正の日数です）。広げたいとき以外は省略してください |

`simple_rule()` は同じモデルの簡易表記で、ルールテキスト
`＋登録、毎月（日付）、1日、休止日 後シフト、相対 4`（月初から 5 営業日目）に
対応します。`相対 n` は*確定後の*アンカーから `n` 営業日を数え、アンカー自身を
0 番目とします。したがって `1日` に対する `相対 4` が 5 営業日目、`L日` に対する
`相対 -2` が月末の 3 営業日前になります。休止日シフトが先にアンカーを確定させ、
`相対` はその確定した日から `相対` 自身の向きに数えます。一方 `verbose_rule()` は
先行する振り替えを行わず、ルールが指定したアンカーに起算を適用します。

移動する段は*1 つだけ*である点に注意してください。`simple_rule()` は振り替えに
よる移動を禁止し、オフセットに全距離を担わせます。2 つを連結すると、範囲内の
休業日 1 日ごとに 2 ステップかかってしまいます。

`相対` と振り替えはいずれも移動であるため、結果が月末をまたぐことがあります。
`day="L", shift="prev", relative=1` は文字どおり「月末の翌営業日」を意味します。
開始年月はルールが*アンカーする*月を画すため、それを遡る日付は拒否しますが、
それを越えて進む日付は拒否しません。`base_day` を指定した場合も同様で、月末の
実行が翌月に着地するのは設計どおりです。

```python
# 月末業務日報：26 日始まりの各営業月の最終営業日
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    base_day=26,
    rules=[nth_business_day_from_end(0)],
)
```

### 開始日の基準はどこか

種別と開始日は独立ではありません。日付を何から数えるかは種別が決めます。この
2 軸は `base_day=1` のとき必ず同じ答えに潰れるため、混同しやすい箇所です。

| 種別 | 日付指定 | 月末指定 | 曜日指定 |
|---|---|---|---|
| `ABSOLUTE` 絶対日 | **暦月** | **暦月**の最終日 | **暦月**内の週 |
| `RELATIVE` 相対日 | **基準日**（1 始まり） | **期間**の最終日 | **基準日から**数えた週 |
| `OPERATING` 運用日 | 期間内の第 n 運用日 | **期間**の最終運用日 | — |
| `CLOSED` 休業日 | 期間内の第 n 休業日 | **期間**の最終休業日 | — |

ここで「期間」とは基準日が定義する営業月のことです。`base_day=26` では
2026-08-26 に開く期間が **2026-09-25** に閉じるため、次のようになります。

```python
# 月末営業日：*期間*の最終営業日 -> 2026-09-25
CalendarTimetable(calendar_id="JP", hour=21, base_day=26,
                  rules=[nth_business_day_from_end(0)])

# 絶対日 + 月末指定 は暦月の読みを維持 -> 2026-08-31
CalendarTimetable(calendar_id="JP", hour=21, base_day=26,
                  rules=[verbose_rule(kind=Kind.ABSOLUTE,
                                      start_day=StartDay.MONTH_END, day=0)])
```

曜日指定にも同じ分岐があります。`ABSOLUTE` は暦月の 1 日から週を数え、
`RELATIVE` は基準日から数えます。したがって `base_day=26` では「第 1 月曜」は
`RELATIVE` で 2026-08-31、`ABSOLUTE` で 2026-08-03 になります。期間内に存在
しない第 N 曜日は、次の期間へはみ出すのではなく期間の最終日に寄せます。

`base_day=1`（既定）にすれば両方の読みは同じものです。基準日が 1 日そのものに
なるためです。つまり、この違いが表に出るのは基準日を設定したときだけです。

### Airflow なしでルールを使う

`airflow_timetables_calendar.rules` と `.calendars` は Airflow を一切
インポートしないため、単なる日付計算機として、あるいは別のスケジューラの内部で
使えます。Airflow をインストールせずに単体テストされ、CI が AST を解析して
その状態を保っています。

1 点注意があります。*パッケージ*をインポートすると Airflow もインポートされ
ます。`__init__` が `CalendarTimetable` を再エクスポートしているためです。
つまりルールエンジンが再利用できるのは、どのみち Airflow が入っている環境に
限られます。避けられるのは依存であって、インストールそのものではありません。

```python
from datetime import date

# パッケージ経由（Airflow のインストールが必要）
from airflow_timetables_calendar import nth_business_day, period_for, ScheduleRule

class MyCalendar:
    def is_working_day(self, day: date) -> bool:
        return day.weekday() < 5

rule = ScheduleRule(**nth_business_day(5))
rule.resolve(period_for(date(2026, 9, 1)), MyCalendar())  # 2026-09-07
```

`is_working_day(day)` を持つオブジェクトなら何でも `WorkingDayCalendar`
プロトコルを満たします。このライブラリからのインポートは不要なので、ホスト側の
スケジューラは自前のカレンダーオブジェクトをそのまま渡せます。

## シリアライズ

カスタム Timetable は、DAG がデシリアライズされるときに Airflow のあらゆる
コンポーネントから解決可能でなければなりません。パッケージをインストールする
だけで十分です。`airflow.plugins` エントリポイントが Timetable を登録し、DAG
シリアライザにそのエンコード方法を教えます。

## 48 時間制

`hour` は `0`〜`23` だけでなく `-47`〜`47` を受け付けます。`0`〜`23` 以外の値は
隣の暦日に実行されますが、所属する**業務上の日付は宣言した日のまま**です。これに
より「月末営業日の翌日 01:00 に実行し、その実行は月末営業日に属する」といった
指定が可能になります。

| `hour` | 実行時刻 | 業務上の日付 |
|---|---|---|
| `21` | 当日 21:00 | 当日 |
| `24` | 翌日 00:00 | 当日 |
| `25` | 翌日 01:00 | 当日 |
| `47` | 翌日 23:00 | 当日 |
| `-1` | 前日 23:00 | 当日 |
| `-24` | 前日 00:00 | 当日 |

```python
# 月末営業日の翌日 01:00 に実行
CalendarTimetable(calendar_id="JP", hour=25, rules=["last_business_day_of_month"])

# 営業日の前日 23:00 に実行
CalendarTimetable(calendar_id="JP", hour=-1, rules=["every_business_day"])
```

ルールの判定は常に業務上の日付に対して行われるため、祝日・営業日の判定は
オフセットの影響を受けません。ずれるのは Airflow に渡される実行時刻だけです。

## 未実装の機能

ここで扱うルール語彙は古典的な挙動の*モデル*であり、特定製品の完全な再実装では
ありません。以下は意図的に欠いています。README に書いているのは、残りの説明が
成立するようにするためです。

- **基準時刻の任意設定**。48 時間制は `hour` でサポートしています
  （[48 時間制](#48-時間制)参照）が、日付の切り替わり点は `timezone` の深夜 0:00 に
  固定されます。業務上の日付が例えば 08:00 に始まる製品の挙動は表現できません。
  実行時刻をずらしたい場合は `hour` を使ってください。
- **有効期日**。開始年月はルールを*下側*からのみ画します。上限がないため、
  猶予日数と有効期日の相互作用も存在しません。古典モデルでは猶予日数が有効期日を
  上書きできますが、ここではその状況自体が発生しません。
- **登録日**。`Kind.REGISTERED` は期間の開始日に解決されます。これは「登録された
  日」の代用であって、実際の登録タイムスタンプではありません。Airflow の実行
  状態とは結び付いていません。
- **振り替え猶予日数の範囲**。古典モデルは 1〜31 日の範囲を規定していますが、
  `grace_days` にレンジチェックはなく、`grace_days=0` は 0 日ではなく既定の
  猶予日数を選びます。
- **運用日 / 休業日 の曜日指定**。仕様の開始日表は曜日指定を絶対日と相対日について
  のみ定義しているため、運用日 / 休業日の 2 通りの組み合わせは未定義です。本
  パッケージはこれらを受け付け、暦月を基準に解決しますが、これは文書化された
  挙動ではなくローカルな選択です。

これらが必要になった場合、`rules.py` の各段が自己完結しているので、次に追加する
のに自然な箇所です。

## 対応範囲と互換性

- Airflow 3.x で検証しています。Timetable は SDK の `BaseTimetable` ではなく
  **コア**の `CronTriggerTimetable` を継承しています。SDK の基底クラスには、
  コア側のコードが無条件に読む属性が欠けているためです（詳細は
  `timetable.py` を参照）。
- シリアライザフックは Airflow の非公開属性を使っています。ガード済みで、
  非対応の Airflow ではスケジューラを落とすのではなく明確なエラーをログに
  出力します。
- Apache Software Foundation とは無関係であり、承認も受けていません。
  "Airflow" は説明的に使用しています。

## ライセンス

MIT。[`LICENSE`](LICENSE) を参照してください。

## 出典

ルール語彙とその境界ケースは、古典的な国産ジョブスケジューラが共有している慣習
（種別 / 開始日 / 休業日の振り替え / 起算スケジュール / 猶予日数、および簡易な
休止日・相対の表記）に従っています。この語彙は公開標準ではなく業界の慣習である
ため、本実装は文書化された挙動の忠実な*モデル*であり、特定製品の設定ファイルに
対するバイト互換のパーサではありません。ベンダーのドキュメントや製品名は一切
転載していません。
