# websocket-ticker-to-candle

## Please note

While the gist of this repository is fully functional ticker streaming setup which can stream websocket data and save the resampled one minute <b>OHLC</b> to db.
Volume data is not stable where as OHLC data is verified.

> Currently streaming of websocket for symbols is done only for <b>Alice Blue</b>

## Prerequisites

> Runs only in Python3.X

## How to RUN

Run below command in terminal activating the environment where you've all dependencies installed.

```
python3 stream_ws.py

or

python stream_ws.py
```

## Databases

<b>SQLITE : </b> sqlite is used to store all the ticks for all the symbols

<b>MYSQL : </b> MySql is used to store all the re-sampled one minute OHLC data into respective symbol table, you can replace mysql with any database of your choice.

Make sure you've <b>dependencies</b> installed ahead following requirements.txt

```
websocket-ticker-to-candle
│
├── credentials
│    ├── env_crendentials.json
|    └── access_tokens.json ( This is auto updated day basis)
├──── data
│    └── stock_data_db.db
├────systemutil
│    └── util.py
│    └── tel_msgr.py
└─── stream_ws.py

```

## Telegram notifications

The below line send's a message during the beginning of the websocket.

Also you could you it your own diff purposes.
If you're willing to use,make sure you've created your own bot using [BotFather](https://telegram.me/BotFather)
Telegram is altogether a different. ocean.

> If you not interested you can just DELETE/ Comment the line.

```
tel_utl.sendMsg("1> Job execution begun at {0}".format(
            datetime.datetime.now()))
```

## Dependent Libraries

### Alice Blue: https://github.com/krishnavelu/alice_blue
