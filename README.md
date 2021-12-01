# Создание графиков для отслеживания состояния инвестиционного портфеля
Реализация на R, работает с API Tinkoff или со сделками, поднятыми из CSV. Посильные комментарии в коде есть, но детали того, почему что-то сделано так, а не иначе, описывать не готов.<br>
<br>
Важно: это некоммерческая разработка, делал для собственного удовольствия (вспомнить, как вообще код пишут). Так что не ждите невероятно хорошей реализации, быстрых алгоритмов или супер-аналитики. Автор ничего никому не обещает, тем более, что основная работа – совсем про другое.<br>
<br>
Источники данных:<br>
https://tinkoffcreditsystems.github.io/invest-openapi/<br>
https://eodhistoricaldata.com/financial-apis/<br>
<br>
Что умеет делать:<br>
1 – подневный срез: СЧА, прибыль, прирост, прибыль против инвестиций (и индексов), дивидендный поток<br>
2 – понедельный срез: динамика оценки бумаг в портфеле по неделям<br>
3 - помесячный срез: дивденды, прирост прибыли, прибыль, СЧА<br>
4 – историческая прибыль по компаниям (за жизнь портфеля), в % и рублях<br>
5 – прогноз стоимости портфеля в будущее<br>
6 – подневный анализ gap-to-target (сколько осталось до целей по бумагам в портфеле)<br>
7 – аналитика по всем портфелям, сводная – доля прибыли в СЧА, подневный, помесячый и понедельный рост прибыли<br>
<br>
Известные баги:<br>
1 – нет облигаций (заменяются на кэш-операции)<br>
2 – нет ETF (заменяются на кэш-операции)<br>
3 – нет фьючерсов (также заменяются на кэш-операции)<br>
4 – не придумал пока, как нормально отображать короткие позиции в недельном чарте, показывающем доли акций в портфеле<br>
5 – 100% будут некоторые расхождения с реальностью, так как API Тинькова не полностью отдаёт комиссии (биржи)<br>
<br>
Состав файлов:<br>
_targets.csv – цели по позцииям в портфеле<br>
_oom-raiffeisen.csv – портфель в Райфе (настоящий)<br>
__market 7.2.R - собственно, сам R-скрипт<br>
