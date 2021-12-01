  # Portfolio calculations, public version

  # prep work (libraries, API URLs, data structures)
  
  library(jsonlite)
  library(httr)
  library(lubridate)
  library(quantmod)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(ggplot2)
  library(ggthemes)
  library(magrittr)
  library(readr)
  options(scipen=999)
  
  # tinkoff API links
  # documentation: https://tinkoffcreditsystems.github.io/invest-openapi/
  
  api_key = "Bearer " # YOUR API KEY TO TINKOFF HERE
  api_operations = "https://api-invest.tinkoff.ru/openapi/operations"
  api_my_portfolio = "https://api-invest.tinkoff.ru/openapi/portfolio"
  api_my_currencies = "https://api-invest.tinkoff.ru/openapi/portfolio/currencies" 
  api_currencies = "https://api-invest.tinkoff.ru/openapi/market/currencies"
  api_stocks = "https://api-invest.tinkoff.ru/openapi/market/stocks"
  api_bonds = "https://api-invest.tinkoff.ru/openapi/market/bonds"
  api_etfs = "https://api-invest.tinkoff.ru/openapi/market/etfs"
  api_accounts = "https://api-invest.tinkoff.ru/openapi/user/accounts" 
  api_prices = "https://api-invest.tinkoff.ru/openapi/market/candles"
  api_ticker = "https://api-invest.tinkoff.ru/openapi/market/search/by-figi"
  
  # eod historical data APIs
  # https://eodhistoricaldata.com/financial-apis/
  
  mcd_api_key = "" # YOUR API KEY FOR EODHISTORICALDATA HERE
  mcd_price_url = "https://eodhistoricaldata.com/api/eod/"
  mcd_splits_url = "https://eodhistoricaldata.com/api/splits/"
  
  # home directory for operating files, and operating file names  
  home = "~/Dropbox/_Personal Data/_investments/investment data/"
  target_csv = "_targets.csv"

  # service variables
  start_date = as.Date("2018-12-26") # start date (operations history depth for APIs)
  end_date = today() # typically, today
  ltm_date = today() - 365 # used for LTM portfolio calculations (to trim overly long charts)
  timeseries <- seq(as.Date(start_date), length = end_date - start_date + 1, by = "days") # used to fill data gaps in non-trading days
  timeseries <- xts(x = timeseries, order.by = timeseries)
  
  # CSV-based portfolio (in this case, Raiffeisen)
  oom_raiffeisen <- as_tibble(read.csv("~/Dropbox/_Personal Data/_investments/investment data/_oom-raiffeisen.csv"))
  oom_raiffeisen$date <- as.Date(oom_raiffeisen$date)
  
  # CSV-based portfolio (off-market operations are not tracked by Tinkoff APIs)
  oom_tcs <- as_tibble(read.csv("~/Dropbox/_Personal Data/_investments/investment data/_oom-tcs.csv"))
  oom_tcs$date <- as.Date(oom_tcs$date)
  
  # get RTS, IMOEX, SPX, NASDAQ indexes
  # recalculate dynamics starting LTM date
  # currently only MOEX and NASDAQ are used on charts
  
  index_rts <- paste(mcd_price_url, "RTSI.indx", "?api_token=", mcd_api_key, "&from=", ltm_date, "&to=", end_date, "&fmt=json", sep = "")
  index_rts <- as_tibble(fromJSON(rawToChar((GET(index_rts)$content))))
  index_rts %<>% mutate(pct = close / first(close) - 1, date = as.Date(date))
  
  index_moex <- paste(mcd_price_url, "IMOEX.indx", "?api_token=", mcd_api_key, "&from=", ltm_date, "&to=", end_date, "&fmt=json", sep = "")
  index_moex <- as_tibble(fromJSON(rawToChar((GET(index_moex)$content))))
  index_moex %<>% mutate(pct = close / first(close) - 1, date = as.Date(date))
  
  index_sp500 <- paste(mcd_price_url, "sp500tr.indx", "?api_token=", mcd_api_key, "&from=", ltm_date, "&to=", end_date, "&fmt=json", sep = "")
  index_sp500 <- as_tibble(fromJSON(rawToChar((GET(index_sp500)$content))))
  index_sp500 %<>% mutate(pct = close / first(close) - 1, date = as.Date(date))
  
  index_nasdaq <- paste(mcd_price_url, "ndx.indx", "?api_token=", mcd_api_key, "&from=", ltm_date, "&to=", end_date, "&fmt=json", sep = "")
  index_nasdaq <- as_tibble(fromJSON(rawToChar((GET(index_nasdaq)$content))))
  index_nasdaq %<>% mutate(pct = close / first(close) - 1, date = as.Date(date))
  
  
  # getting exchange rates and filling gaps in non-trading days
  getSymbols("USDRUB=X", from= start_date, to = end_date)
  `USDRUB=X` <- merge(timeseries, `USDRUB=X`, join = 'left', fill = na.locf)[-c(2)]
  `USDRUB=X` <- na.locf(`USDRUB=X`, fromLast = T)
  usdrub <- as_tibble(data.frame(date = index(`USDRUB=X`), coredata(`USDRUB=X`))[-c(2,3,4,5,7,8)])
  names(usdrub) <- c("date", "usdrub")
  getSymbols("EURRUB=X", from= start_date, to = end_date)
  `EURRUB=X` <- merge(timeseries, `EURRUB=X`, join = 'left', fill = na.locf)[-c(2)]
  `EURRUB=X` <- na.locf(`EURRUB=X`, fromLast = T)
  eurrub <- as_tibble(data.frame(date = index(`EURRUB=X`), coredata(`EURRUB=X`))[-c(2,3,4,5,7,8)])
  names(eurrub) <- c("date", "eurrub")

  # get all bonds, stocks, and ETFs traded in Tinkoff
  # used further to get names of traded securities
  m_bonds <- as_tibble(fromJSON(rawToChar((GET(api_bonds, add_headers(Authorization = api_key)))$content))$payload$instruments)
  m_stocks <- as_tibble(fromJSON(rawToChar((GET(api_stocks, add_headers(Authorization = api_key)))$content))$payload$instruments)
  m_etfs <- as_tibble(fromJSON(rawToChar((GET(api_etfs, add_headers(Authorization = api_key)))$content))$payload$instruments)

  # get account list from Tinkoff and start processing data
  accounts = GET(api_accounts, add_headers(Authorization = api_key))
  accounts = fromJSON(rawToChar(accounts$content))$payload$accounts
  accounts$In = accounts$Out = accounts$Balance = accounts$Dividends = accounts$Coupons = accounts$InPlay = accounts$Commissions = accounts$Taxes = accounts$Value = accounts$Earned = accounts$Pct = 0;
  accounts$EarnedStocks = accounts$EarnedBonds = accounts$EarnedEtfs = accounts$PctStocks = accounts$PctBonds = accounts$PctEtfs = 0;
  
  # add out-of-market accounts (CSV-based portfolios)
  accounts[3, ] <- c("000-Raiffeisen")
  accounts[3, 3:19] <- c(0)
  
  # add targets for securities from a CSV file
  # helps build gap-to-target charts
  targets <- as_tibble(read.csv(paste(home, target_csv, sep = "")))
  targets %<>% mutate(horizon = as.Date(horizon))

  # start processing information in individual accounts
  for (index in 1:nrow(accounts))
  {
    
    # get operations
    # not really pretty: if we have a CSV account in account list, use CSV as a data source
    # otherwise, get operations from Tinkoff API
    
    if (accounts[index, ]$brokerAccountId == "000-Raiffeisen") { ops <- oom_raiffeisen } else {
    	ops = GET(api_operations, 
    		query=list(brokerAccountId = accounts[index, "brokerAccountId"],
    		from=paste(start_date, "T00:00:00+03:00", sep=""), 
    		to=paste(end_date, "T23:59:59+03:00", sep="")), 
    		add_headers(Authorization = api_key))
    			
    	ops = as_tibble(fromJSON(rawToChar(ops$content))$payload$operations)
      ops$date <- as.Date(ops$date)
      
      ops %<>% filter(status == "Done") # removes all incomplete operations
      ops$name = ops$ticker = "" # adds name and ticker to operations list
    }
    
    start_date = min(ops$date) # we start at the time of earliest operation
    timeline <- as_tibble(seq(as.Date(start_date), length = end_date - start_date + 1, by = "days")) # used to fill gaps in data in non-trading days
    names(timeline) <- c("date")
    
    # Tinkoff API is imperfect, and does not possess methods to get operations that require Qualified Investor rating
    # hence, we have to track these operations in the separate CSV file, and attach them to general operations list
    if (accounts[index, ]$brokerAccountId == "2003966260") { ops <- full_join(ops, oom_tcs) }
    
    # another problem with TCS API is that it does not have price history, only candles
    # hence, we cannot track any operations with bonds or ETFs
    # which means we can either remove these operations (not ideal as we lose account balance),
    # or, we can convert these operations into cash ops: reduce balance when we buy ETF or Bond, increase balance when we sell ETF or Bond
    
    ops %<>% mutate (figi = ifelse(str_detect(instrumentType, "Bond|Etf"), "", figi))
    ops %<>% mutate (operationType = ifelse(str_detect(instrumentType, "Bond|Etf") & str_detect(operationType, "TaxCoupon|Buy|BrokerCommission"), "PayOut", operationType))
    ops %<>% mutate (operationType = ifelse(str_detect(instrumentType, "Bond|Etf") & str_detect(operationType, "Coupon|Sell"), "PayIn", operationType))
    ops %<>% mutate (instrumentType = ifelse(str_detect(instrumentType, "Bond|Etf"), "Currency", instrumentType))
    ops %<>% mutate(quantityExecuted = ifelse(operationType == "Sell", -quantityExecuted, quantityExecuted))

    # join ops with ticker data
    # basically, we attach ticker name and security name to operations list
    # and do some clean-up afterwards
    
    ops <- left_join(ops, m_stocks, by = "figi", suffix=c("", "_stocks"))
    ops <- left_join(ops, m_bonds, by = "figi", suffix=c("", "_bonds"))
    ops <- left_join(ops, m_etfs, by = "figi", suffix=c("", "_etfs"))

    ops[!is.na(ops$ticker_stocks), ] %<>% mutate (ticker = ticker_stocks, name = name_stocks)
    ops[!is.na(ops$ticker_bonds), ] %<>% mutate (ticker = ticker_bonds, name = name_bonds)
    ops[!is.na(ops$ticker_etfs), ] %<>% mutate (ticker = ticker_etfs, name = name_etfs)
    
    # MOEX securities have a .MCX ending in eodhistoricaldata.com
    # hence, for all securities traded in RUB we attach a ".MCX" to the ticker name
    ops[ops$currency == "RUB" & ops$ticker != "", ] %<>% mutate(ticker = paste(ticker, "MCX", sep="."))

    # also, there's a lot of bullshit in security naming in TCS API
    # which means we really have to correct some of the names manually
    # sorry, guys :(
    
    ops %<>% select(operationType, date, instrumentType, figi, payment, currency, quantityExecuted, price, name, ticker)
    ops %<>% mutate (ticker = ifelse(ticker == "TCS", "TCS.LSE", ticker))
    ops %<>% mutate (ticker = ifelse(currency == "EUR", str_replace(ticker, "@", "."), ticker))
    ops %<>% mutate (ticker = str_replace(ticker, "@GS", ".LSE"))
    ops %<>% mutate (ticker = str_replace(ticker, ".DE", ".F"))
    ops %<>% mutate (ticker = ifelse(ticker == "TCS.MCX", "TCSG.MCX", ticker))

    # build cumulative balance of a security over total portfolio period
    # e.g. we start with 1, then we buy another 2, then we sell 1, etc
    # this tracks security balance through time
    
    ops[is.na(ops$quantityExecuted), ]$quantityExecuted = 0
    ops %<>% group_by(ticker) %>% arrange(date) %>% mutate(balance = cumsum(quantityExecuted))
    
    # for each unique ticker
    # get every ticker price history, and adjust it for splits
    # place it on a timeline
    # fill balance
    # left join with core list of operations by date

    tickerlist <- ops %>% group_by(ticker) %>% summarise(date = first(date)) # gets all unique tickers from operations, and a trading start date for data requests
    core <- ops[1, ] # future core dataset for further manipulations, this is just to set its structure
    
    for (pin in 2:nrow(tickerlist)) # starts with 2nd position, because position 1 is basically cash (empty)
    {
      print(paste("Getting symbol: ", tickerlist[pin, "ticker"], sep = "")) # simply tracks data requests
      
      # sorry, guys, data is imperfect
      # my way to deal with this: do exceptions
      # CIAN (MOEX CIAN) does not exist in eodhistoricaldata.com, hence, replaced it with another security
      # same goes for SPB Exchange (SPBE ticker)
      # RDS.A has to be managed manually, as ticker names are not the same in TCS and in EOD-data
      
      if (tickerlist[pin, "ticker"] == "CIAN.MCX") { tickerlist[pin, "ticker"] = "OZON.MCX" }
      if (tickerlist[pin, "ticker"] == "SPBE") { tickerlist[pin, "ticker"] = "OZON.MCX" }
      if (tickerlist[pin, "ticker"] == "RDS.A") { tickerlist[pin, "ticker"] = "RDS-A.US" }
      
      # data request
      tickerdata <- paste(mcd_price_url, tickerlist[pin, ]$ticker, "?api_token=", mcd_api_key, "&from=", tickerlist[pin, ]$date, "&to=", end_date, "&fmt=json", sep = "")
      tickerdata <- as_tibble(fromJSON(rawToChar((GET(tickerdata)$content))))
      tickerdata %<>% mutate(date = as.Date(date))
      tickerdata <- as.xts(x = tickerdata[, -1], order.by = tickerdata$date)

      # attach ticker data to a variable with the name same as ticker
      assign(tickerlist[pin, ]$ticker, tickerdata)
      
    }
    
    # build history of trading and holding on each ticker, including daily prices, whenever balance is > 0
    
    for (pin in 2:nrow(tickerlist))
    {
      print (tickerlist[pin, ]$ticker)
      
      # prep ticker prices, fill gaps of non-trading days
      prices <- eval(parse(text = paste("`", tickerlist[pin, ]$ticker, "`", sep = "")))
      prices <- as_tibble(data.frame(date = index(prices), coredata(prices)))[c(1,5)]
      names(prices) <- c("date", "close")
      prices <- left_join(timeline, prices, by="date")
      prices %<>% fill(close, .direction = "downup")
      
      # put all information we have on this ticker on a timeline (same for all account)
      # fill gaps in non-trading days
      # add new operation type ('Hold') to reflect days with zero changes in security balance
      # add usd and eur rates (we calculate portfolio value in RUB), and fill gaps
      
      tickerdata <- left_join(timeline, ops %>% filter(ticker == tickerlist[pin, ]$ticker), by = "date")
      tickerdata %<>% fill(balance, currency, instrumentType, ticker, name, figi, .direction = "down")
      tickerdata %<>% mutate(operationType = ifelse(is.na(operationType), "Hold", operationType))
      tickerdata %<>% filter(!is.na(operationType))
      
      tickerdata <- left_join(tickerdata, prices, by = "date")
      tickerdata <- left_join(tickerdata, usdrub, by = "date")
      tickerdata <- left_join(tickerdata, eurrub, by = "date")
      tickerdata %<>% fill(close, .direction = "down")
      
      tickerdata[is.na(tickerdata$payment), ]$payment <- 0
      tickerdata %<>% mutate(close = as.double(close))
      
      # calculate daily value of a security position
      tickerdata %<>% mutate(value = balance * close, value_rub = balance * close)
      tickerdata %<>% mutate(value_rub = ifelse(currency == "EUR", value * eurrub, value_rub))
      tickerdata %<>% mutate(value_rub = ifelse(currency == "USD", value * usdrub, value_rub))
      
      # calculate daily investment balance (how much was invested into this security)
      tickerdata %<>% mutate(investment = cumsum(payment), investment_rub = cumsum(payment))
      tickerdata %<>% mutate(investment_rub = ifelse(currency == "USD", investment * usdrub, investment_rub))
      tickerdata %<>% mutate(investment_rub = ifelse(currency == "EUR", investment * eurrub, investment_rub))
      
      # calculate daily profit, and daily value dynamics
      tickerdata %<>% mutate(profit = value + investment, profit_rub = value_rub + investment_rub)
      tickerdata %<>% mutate(delta = profit - lag(profit), delta_rub = profit_rub - lag(profit_rub))
      tickerdata[is.na(tickerdata$delta), ]$delta <- 0
      tickerdata[is.na(tickerdata$delta_rub), ]$delta_rub <- 0
      
      # attach this information to core dataset
      core <- rbind(core, tickerdata)
    }

    core <- core[-1, ]
    core %<>% arrange(date)
    
    # now we need to build cash balance, fill up missing dates, and add this balance to core
    # rules: we keep USD, EUR and RUB balances
    # for exchange operations (e.g. RUB -> USD), we split Buy and Sell (double the real operations)
    # FIGI for USD = 
    
    currencies <- ops %>% filter(str_detect(operationType, "PayIn|PayOut"))
    currencies %<>% mutate(ticker = ifelse(currency == "RUB", "RUBTOM", ticker), name = ifelse(currency == "RUB", "Russian roubles", name))
    currencies %<>% mutate(ticker = ifelse(currency == "EUR", "EURTOM", ticker), name = ifelse(currency == "EUR", "Euros", name))
    currencies %<>% mutate(ticker = ifelse(currency == "USD", "USDTOM", ticker), name = ifelse(currency == "USD", "United States dollars", name))
    currencies %<>% mutate(instrumentType = "Currency", quantityExecuted = payment)
    
    exchange_in <- ops %>% filter(instrumentType == "Currency") 
    exchange_out <- ops %>% filter(instrumentType == "Currency")
    
    exchange_in %<>% mutate(ticker = ifelse(figi == "BBG0013HGFT4", "USDTOM", ticker), 
                            name = ifelse(figi == "BBG0013HGFT4", "United States dollars", name), 
                            currency = ifelse(figi == "BBG0013HGFT4", "USD", currency), 
                            payment = quantityExecuted)
    
    exchange_out %<>% mutate(ticker = ifelse(figi == "BBG0013HGFT4", "RUBTOM", ticker), 
                             name = ifelse(figi == "BBG0013HGFT4", "Russian roubles", name), 
                             currency = ifelse(figi == "BBG0013HGFT4", "RUB", currency), 
                             quantityExecuted = ifelse(figi == "BBG0013HGFT4", payment, quantityExecuted), 
                             price = ifelse(figi == "BBG0013HGFT4", 1, price), 
                             operationType = ifelse(operationType == "Buy", "Sell", "Buy"))
    
    currencies <- rbind(currencies, exchange_in, exchange_out)
    
    daily_currencies <- core %>% filter(str_detect(operationType, "Buy|Sell")) %>% group_by(date, currency) %>% summarise(quantityExecuted = sum(payment)) %>% mutate (name = "", ticker = "")
    daily_currencies %<>% mutate(instrumentType = "Currency", operationType = ifelse(quantityExecuted < 0, "Sell", "Buy"), payment = quantityExecuted)
    daily_currencies %<>% mutate(ticker = ifelse(currency == "RUB", "RUBTOM", ticker), name = ifelse(currency == "RUB", "Russian roubles", name), )
    daily_currencies %<>% mutate(ticker = ifelse(currency == "EUR", "EURTOM", ticker), name = ifelse(currency == "EUR", "Euros", name))
    daily_currencies %<>% mutate(ticker = ifelse(currency == "USD", "USDTOM", ticker), name = ifelse(currency == "USD", "United States dollars", name))
    
    currencies <- rbind(currencies, daily_currencies)
    currencies %<>% arrange(date)
    
    currencies %<>% group_by(ticker) %>% mutate(balance = cumsum(quantityExecuted))
    currencies %<>% group_by(date, ticker) %>% summarise (operationType = "Hold", instrumentType = instrumentType, figi = figi, currency = currency, ticker = ticker, name = name, balance = last(balance))
    
    for (pin in 1:n_distinct(currencies$ticker))
    {
      currencyname <- left_join(timeline, currencies %>% filter(ticker == unique(currencies$ticker)[pin]), by = "date")
      currencyname <- left_join(currencyname, usdrub, by = "date")
      currencyname <- left_join(currencyname, eurrub, by = "date")
      currencyname %<>% fill(instrumentType, currency, ticker, name, balance, usdrub, eurrub, .direction = "downup")
      currencyname[is.na(currencyname$operationType), ]$operationType = "Hold"
      
      # calculate rub values (yeah, we can earn or lose money on currency)
      currencyname %<>% mutate(value = balance, value_rub = ifelse(currency == "USD", usdrub * balance, ifelse(currency == "EUR", eurrub * balance, balance)), profit_rub = 0)
      currencyname %<>% mutate (profit_rub = ifelse(ticker == "USDTOM", balance * (usdrub - lag(usdrub)), profit_rub))
      currencyname %<>% mutate (profit_rub = ifelse(ticker == "EURTOM", balance * (eurrub - lag(eurrub)), profit_rub))
      currencyname %<>% mutate(delta_rub = profit_rub - lag(profit_rub))
      
      if (pin == 1) { daily_currencies <- currencyname } else { daily_currencies <- rbind (daily_currencies, currencyname) }
    }
    
    currencies <- daily_currencies
    currencies[is.na(currencies$profit_rub), ]$profit_rub <- 0
    currencies[is.na(currencies$delta_rub), ]$delta_rub <- 0
    currencies$delta = currencies$investment = currencies$investment_rub = currencies$profit = 0
    currencies %<>% arrange(date)
    
    # add currency balances to core operations set
    core <- rbind(core, currencies)
    core %<>% filter(ticker != "")
    core %<>% arrange(date)
    core %<>% mutate(name = ifelse(ticker == "TCS.LSE", "TCS LONDON", name))
    
    
    # build stats by each investment: investment, profit, current value, and % income
    company_stats <- core %>% group_by(ticker, name, currency) %>% filter(str_detect(operationType, "Buy")) %>% summarise(payment = payment, buymoney = -payment, usdrub = usdrub, eurrub = eurrub)
    company_stats %<>% mutate(buymoney = ifelse(currency == "USD", -payment * usdrub, buymoney))
    company_stats %<>% mutate(buymoney = ifelse(currency == "EUR", -payment * eurrub, buymoney))
    company_stats <- company_stats %>% group_by(ticker, name) %>% summarise(buymoney = sum(buymoney))
    company_stats <- left_join(company_stats, core %>% group_by(ticker) %>% summarise(profit_rub = last(profit_rub), value_rub = last(value_rub)), by = "ticker")
    company_stats %<>% mutate(pctIncome = profit_rub / buymoney, name = substr(name, 1, 15))
    company_stats %<>% mutate(color = ifelse(pctIncome < 0, "color1", "color2"))

    # now we have full trading history and current balance, cleaned up and arranged by date
    # time to build daily and monthly stats
    
    # total daily, weekly and monthly stats for current account, filtered by LTM date
    daily_stats <- core %>% group_by(date, ticker) %>% summarise(value_rub = last(value_rub), profit_rub = last(profit_rub), delta_rub = 0, instrumentType = last(instrumentType), balance = last(balance))
    daily_stats %<>% group_by(ticker) %>% mutate(delta_rub = profit_rub - lag(profit_rub))
    
    monthly_stats <- daily_stats %>% group_by(month = floor_date(date, "month"), ticker) %>% summarise(value_rub = last(value_rub), profit_rub = last(profit_rub), delta_rub = 0)
    monthly_stats %<>% group_by(ticker) %>% mutate(delta_rub = profit_rub - lag(profit_rub))
    monthly_stats %<>% mutate(delta_rub = ifelse(is.na(delta_rub), 0, delta_rub))
    monthly_stats %<>% filter (month >= ltm_date)
    
    # weekly stats  
    weekly_stats <- daily_stats %>% group_by(week = floor_date(date, "week"), ticker) %>% 
      summarise(value_rub = last(value_rub), profit_rub = last(profit_rub), delta_rub = 0, instrumentType = last(instrumentType), balance = last(balance))
    weekly_stats %<>% group_by(ticker) %>% mutate(delta_rub = profit_rub - lag(profit_rub))
    weekly_stats %<>% mutate(delta_rub = ifelse(is.na(delta_rub), 0, delta_rub))
    weekly_stats %<>% filter (week >= ltm_date)
    
    # %-based earnings vs invested cash
    
    pct_daily_stats <- ops %>% filter(str_detect(operationType, "Pay")) %>% select(date, currency, payment)
    pct_daily_stats <- left_join(pct_daily_stats, usdrub, by = "date")
    pct_daily_stats <- left_join(pct_daily_stats, eurrub, by = "date")
    pct_daily_stats %<>% mutate(payment = ifelse(currency == "USD", payment * usdrub, ifelse(currency == "EUR", payment * eurrub, payment)))
    pct_daily_stats %<>% select(date, payment)
    pct_daily_stats %<>% mutate(invested = cumsum(payment)) 
    pct_daily_stats <- left_join(timeline, pct_daily_stats, by = "date") 
    pct_daily_stats %<>% fill(invested, .direction = "downup") %>% select(date, invested)
    pct_daily_stats <- left_join(pct_daily_stats, daily_stats %>% group_by(date) %>% summarise(profit = sum(profit_rub)), by = "date")
    pct_daily_stats %<>% mutate(pct = profit / invested)
    pct_daily_stats %<>% filter(date >= ltm_date)
    
    # =========================
    # > chart: invested cash vs total profit vs IMOEX and NASDAQ
    # =========================

    ggplot(pct_daily_stats) +
      geom_line(aes(x = date, y = invested), color = "deeppink") + 
      
      # imoex
      geom_line(data = index_moex, aes(x = date, y = pct * max(pct_daily_stats$invested)), size = 0.5, color = "navyblue", alpha = 0.5) +
      geom_point(data = index_moex %>% filter(date == floor_date(date, "month")), aes(x = date, y = pct * max(pct_daily_stats$invested)), size = 2, color = "navyblue") +
      geom_text(data = index_moex %>% filter(date == floor_date(date, "month")), 
                aes(x = date, y = pct * max(pct_daily_stats$invested), label = scales::percent(pct, accuracy = 1)), size = 3, vjust = -2) +
      geom_label(data = tail(index_moex, 1), aes(x = date, y = pct*max(pct_daily_stats$invested), label = "MOEX"), fill = "navyblue", color = "white") + 
      
      # nasdaq
      geom_line(data = index_nasdaq, aes(x = date, y = pct * max(pct_daily_stats$invested)), size = 0.5, color = "steelblue4", alpha = 0.5) +
      geom_point(data = index_nasdaq %>% filter(date == floor_date(date, "month")), aes(x = date, y = pct * max(pct_daily_stats$invested)), size = 2, color = "steelblue4") +
      geom_text(data = index_nasdaq %>% filter(date == floor_date(date, "month")), 
                aes(x = date, y = pct * max(pct_daily_stats$invested), label = scales::percent(pct, accuracy = 1)), size = 3, vjust = -2) +
      geom_label(data = tail(index_nasdaq, 1), aes(x = date, y = pct*max(pct_daily_stats$invested), label = "NASDAQ"), fill = "steelblue4", color = "white") + 
      
      # account dynamics
      geom_line(aes(x = date, y = profit), color = "darkturquoise", size = 1) +
      geom_point(data = pct_daily_stats %>% filter(date == floor_date(date, "month")), aes(x = date, y = pct * invested), size = 5, color = "darkturquoise") +
      geom_text(data = pct_daily_stats %>% filter(date == floor_date(date, "month")), 
                  aes(x = date, y = pct * invested, label = scales::percent(pct, accuracy = 1)), size = 3, vjust = -2) + 
      geom_point(data = pct_daily_stats %>% filter(date == last(date)), aes(x = date, y = pct * invested), size = 4, color = "darkturquoise") +
      geom_text(data = pct_daily_stats %>% filter(date == last(date)), 
                aes(x = date, y = pct * invested, label = scales::percent(pct, accuracy = 1)), size = 3, vjust = -2) + 
      geom_text(data = pct_daily_stats %>% filter(date == floor_date(date, "month")), 
                aes(x = date, y = invested, label = format(round(invested, 0), big.mark = ",")), size = 3, vjust = -2) + 
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      ggtitle(paste("Прибыль в % против инвестиций, руб, счёт №", accounts[index, "brokerAccountId"], " (LTM)", sep = ""), 
              subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$profit_rub), 0), big.mark = ","), " руб.", 
                               "\nИнвестиции составили ", 
                               format(round(last(pct_daily_stats$invested), 0), big.mark = ","), " руб.", 
                               sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_daily_profit_pct", ".pdf", sep=""), device = cairo_pdf)
    

    # =========================
    # > charts: stats by company, absolute returns and %-based returns
    # =========================
    
    ggplot(company_stats %>% filter(!(profit_rub < 10000 & profit_rub > -10000)), aes(x = profit_rub, y = reorder(name, profit_rub), fill = color)) +
      geom_bar(stat = "identity") +
      geom_text(aes(label = format(round(profit_rub, 0), big.mark = ",", nsmall = 0), x = profit_rub), hjust = 0.9, size = 3) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Lifetime profit, by company, cash-based, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", format(round(sum(company_stats$profit_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, accounts[index, "brokerAccountId"], "---", "_profit_by_company", ".pdf", sep=""), device = cairo_pdf)
    
    ggplot(company_stats %>% filter(!(profit_rub < 10000 & profit_rub > -10000)), aes(x = pctIncome, y = reorder(name, pctIncome), fill = color)) +
      geom_bar(stat = "identity") +
      geom_text(aes(label = scales::percent(pctIncome, accuracy = 1)), hjust = 0.5, size = 3) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
      ggtitle(paste("Lifetime profit, by company, %-based, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", format(round(sum(company_stats$profit_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, accounts[index, "brokerAccountId"], "---", "_profit_by_company-pct", ".pdf", sep=""), device = cairo_pdf)

    
    # =========================
    # > chart: total profit by month, total value by month, profit growth by month
    # =========================
    
    ggplot(monthly_stats, aes(x = month, y = profit_rub, fill = ticker)) +
      geom_bar(stat = "identity", position = position_stack()) +
      geom_text(data = monthly_stats %>% filter (month == floor_date(end_date, "month")), aes(label = ticker), position = position_stack(vjust = 0.5), size = 2) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Прибыль активов по месяцам, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Прибыль активов на конец периода с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$profit_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_monthly_profit", ".pdf", sep=""), device = cairo_pdf)
    
    ggplot(monthly_stats, aes(x = month, y = value_rub, fill = ticker)) +
      geom_bar(stat = "identity", position = position_stack()) +
      geom_text(data = monthly_stats %>% filter (month == floor_date(end_date, "month")), aes(label = ticker), position = position_stack(vjust = 0.5), size = 2) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Стоимость чистых активов по месяцам, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Стоимость чистых активов на конец периода с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$value_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_monthly_value", ".pdf", sep=""), device = cairo_pdf)
    
    monthly_delta_pct <- monthly_stats %>% group_by(month) %>% summarise(delta_rub = sum(delta_rub), value_rub = sum(value_rub))
    monthly_delta_pct <- left_join(monthly_delta_pct, pct_daily_stats %>% group_by(month = floor_date(date, "month")) %>% summarise(invested = last(invested)), by = "month")
    monthly_delta_pct %<>% mutate(pct = delta_rub / lag(value_rub))
    monthly_delta_pct %<>% mutate (pct = ifelse(is.na(pct), 1, pct + 1))
    monthly_delta_pct %<>% mutate (comp = cumprod(pct))
    
    ggplot(monthly_stats, aes(x = month, y = delta_rub, fill = ticker)) +
      geom_bar(stat = "identity", position = position_stack()) +
      geom_text(data = monthly_stats %>% filter (month == floor_date(end_date, "month")), aes(label = ticker), position = position_stack(vjust = 0.5), size = 2) +
      geom_label(data = monthly_delta_pct, fontface = "bold", fill = "green",
                aes(label = paste(format(round(delta_rub/1000, 1), big.mark = ","), " (", scales::percent(pct - 1, 0.1), ")", sep = "")),
                position = position_stack(vjust = 1), size = 3) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Прирост прибыли по месяцам, руб, счёт №", accounts[index, "brokerAccountId"], " (LTM)", sep = ""), 
              subtitle = paste("Данные за период с ", start_date, " по ", end_date, "; прирост последнего месяца: ", 
                               format(round((monthly_stats %>% group_by(month) %>% summarise(delta = sum(delta_rub)) %>% filter (month == floor_date(end_date, "month")))$delta, 0), big.mark = ","),
                               " руб.\n", 
                               "Сложный процент прироста баланса за 12 месяцев (с нормализованной базой) составил +",
                               scales::percent(last(monthly_delta_pct$comp) - 1, accuracy = 1.1), 
                               sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_monthly_delta", ".pdf", sep=""), device = cairo_pdf)
    
    
    # =========================
    # > chart: total profit by week, 100% chart; currencies not included into balance
    # =========================
    
    ggplot(weekly_stats %>% filter(instrumentType != "Currency"), aes(x = week, y = value_rub, fill = ticker)) +
      geom_bar(stat = "identity", position = position_fill()) +
      geom_text(data = weekly_stats %>% 
                  filter (instrumentType != "Currency") %>% 
                  filter (week == last(week)) %>%
                  filter (balance > 0), aes(label = ticker), position = position_fill(vjust = 0.5), size = 3) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Стоимость чистых активов (без кэша) по неделям, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Стоимость чистых активов (с кэшом) на конец периода с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$value_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "weekly_value_100p", ".pdf", sep=""), device = cairo_pdf)
    
    
    # =========================
    # > charts: ltm, total value by day, total profit by day
    # =========================
    
    daily_stats %<>% filter (date >= ltm_date)
    
    ggplot(daily_stats, aes(x = date, y = profit_rub)) +
      geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
      geom_line(data = daily_stats %>% group_by(date) %>% summarise(profit = sum(profit_rub)), aes(x = date, y = profit)) + 
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = c(.02, .98),
            legend.text = element_text(size = 6),
            legend.justification = c("left", "top"),
            legend.box.just = "left",
            legend.title = element_blank(),
            legend.margin = margin(6, 6, 6, 6)) +
      ggtitle(paste("Прибыль активов по дням, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$profit_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_daily_profit", ".pdf", sep=""), device = cairo_pdf)
    
    
    ggplot(daily_stats, aes(x = date, y = value_rub)) +
      geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
      geom_line(data = daily_stats %>% group_by(date) %>% summarise(profit = sum(profit_rub)), aes(x = date, y = profit)) + 
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = c(.02, .98),
            legend.text = element_text(size = 6),
            legend.justification = c("left", "top"),
            legend.box.just = "left",
            legend.title = element_blank(),
            legend.margin = margin(6, 6, 6, 6)) +
      ggtitle(paste("Стоимость чистых активов по дням, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Стоимость активов на конец периода с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$value_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_daily_value", ".pdf", sep=""), device = cairo_pdf)
    
    
    # ==========================
    # > chart: last 90 days in detail, profit and trend
    # ==========================
    
    backdrop <- 90; # can adjust to 30 days or 180 days or any other number
    
    ggplot(daily_stats %>% filter(date >= end_date - backdrop), aes(x = date, y = profit_rub)) +
      geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
      geom_line(data = daily_stats%>% filter(date >= end_date - backdrop) %>% group_by(date) %>% summarise(profit = sum(profit_rub)), aes(x = date, y = profit)) + 
      geom_smooth(data = daily_stats%>% filter(date >= end_date - backdrop) %>% group_by(date) %>% summarise(profit_rub = sum(profit_rub)), method = "lm", se = T, color = "red") +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Прибыль активов по дням, последние ", backdrop, " дней, руб, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", 
                               format(round(sum((daily_stats %>% filter (date == end_date))$profit_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_daily_profit_p", backdrop, "d", ".pdf", sep=""), device = cairo_pdf)
    
    
    # ==========================
    # > chart: dividend flow (cumulative)
    # ==========================
    
    dividend_stats <- core %>% filter(operationType == "Dividend") %>% select(date, ticker, name, currency, payment, usdrub, eurrub, investment_rub) %>%
      mutate(investment_rub = abs(investment_rub), dividend_rub = ifelse(currency == "USD", payment * usdrub, ifelse(currency == "EUR", payment * eurrub, payment))) 
    
    dividend_stats %<>% group_by(date) %>% summarise(total = sum(dividend_rub)) %>% mutate(total_rub = cumsum(total))
    
    ggplot(dividend_stats, aes(x = date, y = total_rub)) + 
      geom_area(fill = "darkturquoise", alpha = 0.7) +
      geom_text(aes(label = format(round(total_rub, 0), big.mark = ","))) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Полученные дивиденды за весь период, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Величина дивидендов на конец периода с ", start_date, " по ", end_date, " составила ", 
                               format(round(last(dividend_stats$total_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_dividends_total", ".pdf", sep=""), device = cairo_pdf)
    
    
    # ==========================
    # > chart: dividend flow (by company, by month)
    # ==========================
    
    dividend_stats_m <- core %>% filter(operationType == "Dividend") %>% select(date, ticker, name, currency, payment, usdrub, eurrub, investment_rub) %>%
      mutate(investment_rub = abs(investment_rub), dividend_rub = ifelse(currency == "USD", payment * usdrub, ifelse(currency == "EUR", payment * eurrub, payment))) 
    
    dividend_stats_m %<>% group_by(date = floor_date(date, "month"), ticker) %>% summarise(dividend_rub = sum(dividend_rub))
    
    ggplot(dividend_stats_m, aes(x = date, y = dividend_rub, fill = ticker)) + 
      geom_bar(stat = "identity") +
      geom_text(aes(label = paste(ticker, format(round(dividend_rub, 0), 0, big.mark = "," ), sep = ": ")), size = 3, position = position_stack(vjust = 0.5)) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.position = "none") +
      ggtitle(paste("Полученные дивиденды за весь период, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Величина дивидендов на конец периода с ", start_date, " по ", end_date, " составила ", 
                               format(round(last(dividend_stats$total_rub), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "____", accounts[index, "brokerAccountId"], "---", "_dividends_monthly", ".pdf", sep=""), device = cairo_pdf)
    
    
    # ==========================
    # > calculations & charts: implied portfolio value
    # ==========================
    
    # select securities that are active in this account
    # get data from targets file (forecasted price and horizon)
    # merge two togehter (left join targets to active securities)
    # calculate compounded percentages (daily)

    implied_stats <- core %>% filter(date == end_date & instrumentType != "Currency" & balance > 0) %>% 
      summarise(value_rub = last(value_rub), currency = last(currency), balance = last(balance), close = last(close), profit_rub = last(profit_rub), close = last(close))
    
    implied_stats <- left_join(implied_stats, targets, by = "ticker") %>% 
      filter(!is.na(target)) %>% # if there is no target for security, remove this security from analysis
      mutate(close = value_rub / balance) %>% 
      mutate(target = ifelse(currency == "USD", target * last(usdrub$usdrub), ifelse(currency == "EUR", target * last(eurrub$eurrub), target))) %>%
      mutate(implied = balance * target, upside = (target - close) * balance, upsidepct = upside / value_rub, duration = horizon - today()) %>%
      mutate(cagr = (target / close) ^ (1 / (as.numeric(duration)/365))) %>% 
      mutate(cdgr = (target / close) ^ (1 / (as.numeric(duration)))) %>% 
      mutate(date = today())
    
    # build daily returns based on uniform compounded daily interest
    
    for (pig in 1:nrow(implied_stats))
    {
      # add time horizon (how far is the most distant forecast)
      days_ahead <- as_tibble(seq(today(), length = max(implied_stats$horizon) - today() + 1, by = "days"))
      names(days_ahead) <- c("date")
      
      # put stats on the timeline, fill gaps, build share price over compounded daily pct, and build valuations
      implied_timeline <- left_join(days_ahead, implied_stats[pig, ], by = "date") %>% 
        select(date, ticker, cdgr, balance, close, target) %>% 
        fill (everything()) %>%
        mutate (price = cumprod(cdgr)) %>%
        mutate (price = close * price, value = price * balance)
      
      # flatten the curve after forecast reaches target
      implied_timeline[as.numeric(implied_stats[pig, ]$horizon - today()) : nrow(implied_timeline), ] %<>% 
        mutate (price = implied_timeline[as.numeric(implied_stats[pig, ]$horizon - today()), ]$price, 
                value = implied_timeline[as.numeric(implied_stats[pig, ]$horizon - today()), ]$value)
      
      if (pig > 1) { implied_data <- rbind(implied_data, implied_timeline) } else { implied_data <- implied_timeline }
    }
    
    # arrange forecast by date, build cumulative value
    implied_data %<>% arrange(date)
    implied_data %<>% group_by(date) %>% mutate(total_value = sum(value))
    
    # build area chart for total portfolio value forecast
    ggplot(implied_data %>% group_by(date) %>% summarise(value = last(total_value))) + 
      geom_area(aes(x = date, y = value), fill = "darkturquoise", alpha = 0.7) + 
      geom_text(data = implied_data %>% group_by(date) %>% summarise(value = last(total_value)) %>% filter(date == last(date)), 
        aes(x = date, y = value, label = format(round(value, 0), big.mark = ",")), size = 3, vjust = -2) +
      scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
      theme_fivethirtyeight() +
      theme(legend.title = element_blank()) +
      ggtitle(paste("Прогноз набора стоимости компаниями, счёт №", accounts[index, "brokerAccountId"], sep = ""), 
              subtitle = paste("Общая стоимость счёта на конец периода (без учёта плеча и кэша): ",  
              format(round(sum(implied_stats$implied), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, accounts[index, "brokerAccountId"], "---implied-income-forecast.pdf", sep=""), device = cairo_pdf)
    
    
    # build comparison: relative upside and projectted CAGR
    ggplot(implied_stats) +
      geom_bar(aes(x = upside / max(upside) * max(cagr), y = name), fill = "darkturquoise", stat = "identity") +
      geom_bar(aes(x = cagr, y = name), fill = "deeppink", stat = "identity", width = 0.3) +
      geom_bar(aes(x = upsidepct, y = name), fill = "yellow", stat = "identity", width = 0.1) +
      geom_label(aes(x = 0, y = name, label = format(round(upside, 0), big.mark = ",")), fill = "white", size = 3, position = position_stack(vjust = 1)) +
      geom_label(aes(x = cagr, y = name, label = scales::percent(cagr, accuracy = 1)), fill = "white", size = 3, position = position_stack(vjust = 1)) +
      theme_fivethirtyeight() + 
      scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
      theme(legend.position = "none") +
      ggtitle(paste("Апсайд, относительный апсайд, CAGR до цели, счёт №", accounts[index, "brokerAccountId"], sep = ""),
              subtitle = "Апсайд зелёный, апсайд относительно остальных участников портфеля жёлтый, CAGR розовый")
    ggsave(width = 12, height = 12, paste(home, accounts[index, "brokerAccountId"], "---implied-income.pdf", sep=""), device = cairo_pdf)
    
    
    # ==========================
    # > chart: gap to target, by date
    # ==========================
    
    target_stats <- core %>% filter(date == end_date & instrumentType != "Currency" & balance > 0) %>% 
      summarise(value_rub = last(value_rub), profit_rub = last(profit_rub), close = last(close)) %>% mutate (pct = profit_rub / (value_rub))
    target_stats <- left_join(target_stats, targets, by = "ticker") %>% filter(!is.na(target)) %>% mutate(gap = close / target)
    target_stats %<>% mutate(color = ifelse(gap < 0.9, "cadetblue1", ifelse(gap < 1, "darkturquoise", "deeppink")))
    target_stats %<>% mutate(color2 = ifelse(pct >= 0, "deeppink", "darkturquoise"))
    
    ggplot(target_stats) +
      geom_bar(aes(x = gap, y = name), fill = target_stats$color, stat = "identity") +
      geom_bar(aes(x = profit_rub/max(profit_rub), y = name), fill = "red", width = 0.2, stat = "identity") +
      geom_bar(aes(x = pct, y = name), fill = "yellow", width = 0.1, stat = "identity") +
      geom_label(aes(x = gap, y = name, label = scales::percent(pct, accuracy = 1), fill = color2), color = "white", size = 3, position = position_stack(vjust = 1)) +
      geom_text(aes(x = gap, y = name, label = scales::percent(gap, accuracy = 1)), size = 3, position = position_stack(vjust = 0.5)) +
      geom_label(aes(x = profit_rub/max(profit_rub), y = name, label = format(round(profit_rub, 0), big.mark = ",", nsmall = 0)), 
                 fill = "white", size = 3, position = position_stack(vjust = 1.05)) +
      theme_fivethirtyeight() + 
      scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
      theme(legend.position = "none") +
      ggtitle(paste("Gap to target по позициям на ", today(), ", руб, счёт №", accounts[index, "brokerAccountId"], sep = ""),
              subtitle = paste("Всего прибыль к этому дню: ",  
                               format(round(sum((core %>% filter(date == today()) %>% summarise(profit = profit_rub))$profit), 0), big.mark = ","), " руб.", sep=""))
    ggsave(width = 12, height = 12, paste(home, "__", accounts[index, "brokerAccountId"], "---gtt-", today(), ".pdf", sep=""), device = cairo_pdf)

        
    # =========================
    # > aggregate data for joint portfolio over all accounts
    # =========================
    
    core$account <- accounts[index, ]$brokerAccountId
    daily_stats$account <- accounts[index, ]$brokerAccountId
    pct_daily_stats$account <- accounts[index, ]$brokerAccountId
    
    if (index == 1) { total <- core; total_d <- daily_stats; total_d_pct <- pct_daily_stats; } 
    else { total <- rbind(total, core); total_d <- rbind(total_d, daily_stats); total_d_pct <- rbind(total_d_pct, pct_daily_stats) }
    
  }
  
  # =========================
  # > build joint portfolio over all accounts
  # =========================
  
  total_d %<>% group_by(date, ticker) %>% summarise(value_rub = sum(value_rub), profit_rub = sum(profit_rub), value_rub = sum(value_rub))
  total_d_pct <- total_d_pct %>% 
    group_by(date, account) %>% 
    summarise(invested = last(invested), profit = last(profit)) %>% 
    group_by(date) %>% summarise(invested = sum(invested), profit = sum(profit))
  
  total_m <- total_d %>% group_by(date = floor_date(date, "month"), ticker) %>% summarise(profit_rub = last(profit_rub), value_rub = last(value_rub))
  total_m %<>% group_by(ticker) %>% mutate (delta_rub = profit_rub - lag(profit_rub))
  total_m %<>% mutate(delta_rub = ifelse(is.na(delta_rub), 0, delta_rub))
  
  total_w <- total_d %>% group_by(date = floor_date(date, "week"), ticker) %>% summarise(profit_rub = last(profit_rub), value_rub = last(value_rub))
  total_w %<>% group_by(ticker) %>% mutate (delta_rub = profit_rub - lag(profit_rub))
  total_w %<>% mutate(delta_rub = ifelse(is.na(delta_rub), 0, delta_rub))
  
  total_d_pct %<>% mutate(value = invested + profit)
  total_d_pct %<>% mutate(pct_profit = profit / value, pct_invested = invested / value)
  
  area_pct <- total_d_pct %>% select(date, pct_profit) %>% mutate(group = "Profit")
  names(area_pct) <- c("date", "pct_invested", "group")
  area_pct <- rbind(area_pct, total_d_pct %>% select(date, pct_invested) %>% mutate(group = "Investments"))
  
  # =========================
  # > converting to LTM
  # =========================
  
  total_d %<>% filter(date >= ltm_date)
  total_m %<>% filter(date >= ltm_date)
  total_w %<>% filter(date >= ltm_date)
  total_d_pct %<>% filter(date >= ltm_date)
  
  # daily profit charts, all portfolios
  ggplot(total_d, aes(x = date, y = profit_rub)) +
    geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
    geom_line(data = total_d %>% group_by(date) %>% summarise(profit = sum(profit_rub)), aes(x = date, y = profit)) + 
    scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
    theme_fivethirtyeight() +
    theme(legend.position = "none") +
    ggtitle("Прибыль по дням, все счета (LTM)", 
            subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", 
                             format(round((total_d_pct %>% filter (date == end_date))$profit, 0), big.mark = ","), " руб.", sep=""))
  ggsave(width = 12, height = 12, paste(home, "_____total_profit_daily.pdf", sep=""), device = cairo_pdf)
  
  # monthly profit charts, all portfolios
  ggplot(total_d %>% group_by(date = floor_date(date, "month"), ticker) %>% summarise(profit_rub = last(profit_rub)), aes(x = date, y = profit_rub)) +
    geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
    geom_line(data = total_d %>% group_by(date) %>% summarise(profit = sum(profit_rub)), aes(x = date, y = profit)) + 
    geom_label(data = total_d %>% group_by(date) %>% summarise(profit = sum(profit_rub)) %>% group_by(date = floor_date(date, "month")) %>% summarise(profit = last(profit)), 
               aes(x = date, y = profit, label = format(round(profit/1000, 1), big.mark = ",")), fontface = "bold", fill = "green") +
    scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
    theme_fivethirtyeight() +
    theme(legend.position = "none") +
    ggtitle("Прибыль по месяцам, все счета (LTM)", 
            subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", 
                             format(round((total_d_pct %>% filter (date == end_date))$profit, 0), big.mark = ","), " руб.", sep=""))
  ggsave(width = 12, height = 12, paste(home, "_____total_profit_monthly.pdf", sep=""), device = cairo_pdf)
  
  total_m_agr <- total_m %>% group_by(date) %>% summarise(delta_rub = sum(delta_rub), profit_rub = sum(profit_rub), value_rub = sum(value_rub)) %>% 
    mutate(pct = delta_rub / lag(value_rub)) %>%
    mutate(pct = ifelse(is.na(pct), 1, pct + 1)) %>% 
    mutate(comp = cumprod(pct)) 
  
  # monthly delta charts, all portfolios
  ggplot(total_m, aes(x = date, y = delta_rub)) +
    geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
    geom_label(data = total_m_agr, fontface = "bold", fill = "green",
               aes(label = paste(format(round(delta_rub/1000, 1), big.mark = ","), " (", scales::percent(pct - 1, 0.1), ")", sep = "")),
               position = position_stack(vjust = 1), size = 3) +
    scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
    theme_fivethirtyeight() +
    theme(legend.position = "none") +
    ggtitle("Прирост прибыли по месяцам, все счета (LTM)", 
            subtitle = paste("Прирост прибыли последнего месяца составил ", 
                             format(round(last(total_m_agr$delta_rub), 0), big.mark = ","), 
                             " руб.\n",
                             "Сложный процент прироста баланса за 12 месяцев (с нормализованной базой) составил +",
                             scales::percent(last(total_m_agr$comp)-1, 0.1), 
                             sep=""))
  ggsave(width = 12, height = 12, paste(home, "_____total_delta_monthly.pdf", sep=""), device = cairo_pdf)
  
  # weekly delta charts, all portfolios
  ggplot(total_w, aes(x = date, y = delta_rub)) +
    geom_bar(aes(fill = ticker), stat = "identity", position = position_stack()) +
    geom_label(data = total_w %>% group_by(date) %>% summarise(delta_rub = sum(delta_rub)), fontface = "bold", fill = "green",
               aes(label = format(round(delta_rub/1000, 1), big.mark = ",")),
               position = position_stack(vjust = 1), size = 3) +
    scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
    theme_fivethirtyeight() +
    theme(legend.position = "none") +
    ggtitle("Прирост прибыли по неделям, все счета (LTM)", 
            subtitle = paste("Прирост прибыли последней недели составил ", 
            format(round((total_w %>% group_by(date) %>% summarise(delta = sum(delta_rub)) %>% filter (date == floor_date(end_date, "week")))$delta, 0), big.mark = ","), " руб.", sep=""))
  ggsave(width = 12, height = 12, paste(home, "_____total_delta_weekly.pdf", sep=""), device = cairo_pdf)
  
  
  # share of profits in total income, all portfolios
  ggplot(area_pct) + 
    geom_area(aes(x = date, y = pct_invested, fill = group)) +
    geom_text(data = area_pct %>% filter(date == floor_date(date, "month"), group == "Profit") , 
              aes(x = date, y = pct_invested, label = scales::percent(pct_invested, accuracy = 1)), size = 3, vjust = -2) + 
    geom_text(data = area_pct %>% filter(date == last(date), group == "Profit") , 
              aes(x = date, y = pct_invested, label = scales::percent(pct_invested, accuracy = 1)), size = 3, vjust = -2) + 
    scale_y_continuous(labels=function(x) format(x, big.mark = ",", scientific = FALSE)) +
    theme_fivethirtyeight() +
    theme(legend.title = element_blank()) +
    ggtitle("Доля прибыли в стоимости активов, руб, все счета, весь период",
            subtitle = paste("Прибыль за период с ", start_date, " по ", end_date, " составила ", 
                             format(round((total_d_pct %>% filter (date == end_date))$profit, 0), big.mark = ","), " руб.", sep=""))
  ggsave(width = 12, height = 12, paste(home, "_____total_daily_pct.pdf", sep=""), device = cairo_pdf)
  
  
  