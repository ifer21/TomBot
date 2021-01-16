
import pandas as pd
import numpy as np
import time
from datetime import datetime
import plotly.graph_objects as go
import logging

def historic_reader(path, symbol):
    # Historic dfs
    data_5m = pd.read_csv(path + 'historic/'+symbol+'-5m-data.csv')
    data_1h = pd.read_csv(path + 'historic/'+symbol+'-1h-data.csv')
    data_1d = pd.read_csv(path + 'historic/'+symbol+'-1d-data.csv')
    # Formatting data
    data_1h['MA9'] = data_1h.close.rolling(window=9, min_periods=1).mean()
    data_1h['MA20'] = data_1h.close.rolling(window=20, min_periods=1).mean()
    data_1h['MA100'] = data_1h.close.rolling(window=100, min_periods=1).mean()
    data_1h.reset_index(inplace=True)
    data_1h['Date_time'] = pd.to_datetime(data_1h['timestamp'])
    data_1h.set_index(data_1h['timestamp'], inplace=True)
    data_5m['MA9'] = data_5m.close.rolling(window=9, min_periods=1).mean()
    data_5m['MA20'] = data_5m.close.rolling(window=20, min_periods=1).mean()
    data_5m['MA100'] = data_5m.close.rolling(window=100, min_periods=1).mean()
    data_5m.reset_index(inplace=True)
    data_5m['Date_time'] = pd.to_datetime(data_5m['timestamp'])
    data_5m.set_index(data_5m['timestamp'], inplace=True)
    data_1d['MA9'] = data_1d.close.rolling(window=9, min_periods=1).mean()
    data_1d['MA20'] = data_1d.close.rolling(window=20, min_periods=1).mean()
    data_1d['MA100'] = data_1d.close.rolling(window=100, min_periods=1).mean()
    data_1d.reset_index(inplace=True)
    data_1d['Date_time'] = pd.to_datetime(data_1d['timestamp'])
    data_1d.set_index(data_1d['timestamp'], inplace=True)
    # Trends df
    trends_df = pd.read_csv(path + 'trendlines_' + symbol + '_py.csv', sep='\t')
    # Entered positions: Make plot entered positions
    return data_5m, data_1h, data_1d, trends_df


def c_plotter(base_df, data_5m, data_1h, data_1d, trends_df, filled_df = [], trend_percen = 0.02):
    logger = logging.getLogger('root')
    # 1h chart plot only last 12 months
    # 5m chart plot only last week
    # 1d chart plot all data
    logger.info(f"Generating plot for {base_df} tf")
    if base_df == '5m':
        delta = '3 day'
        end_date = data_5m.iloc[-1]['Date_time']
        start_date = end_date - pd.Timedelta(delta)
    elif base_df == '1h':
        delta = '365 day'
        end_date = data_1h.iloc[-1]['Date_time']
        start_date = end_date - pd.Timedelta(delta)
    elif base_df == '1d':
        delta = 0
        end_date = data_1d.iloc[-1]['Date_time']
        start_date = data_1d.iloc[0]['Date_time']

    # Masking dataframes
    mask = (data_1h['Date_time'] > start_date) & (data_1h['Date_time'] <= end_date)
    sub_df1h = data_1h.loc[mask]
    mask2 = (data_5m['Date_time'] > start_date) & (data_5m['Date_time'] <= end_date)
    sub_df5m = data_5m.loc[mask2]
    mask3 = (data_1d['Date_time'] > start_date) & (data_1d['Date_time'] <= end_date)
    sub_df1d = data_1d.loc[mask3]
    #mask4 = (base_df['Date_time'] > start_date) & (base_df['Date_time'] <= end_date)
    #sub_df = base_df.loc[mask4]

    if base_df == '5m':
        sub_df = sub_df5m
    elif base_df == '1h':
        sub_df = sub_df1h
    elif base_df == '1d':
        sub_df = sub_df1d


    # 5 min MAs
    MA_5min = [go.Scatter(x=sub_df5m.timestamp, y=sub_df5m.MA9, line=dict(color='blue', width=1.5, dash='dot'), name='MA9 5m',yaxis='y1'),
               go.Scatter(x=sub_df5m.timestamp, y=sub_df5m.MA20, line=dict(color='darkorange', width=1.5, dash='dot'),name='MA20 5m', yaxis='y1'),
               go.Scatter(x=sub_df5m.timestamp, y=sub_df5m.MA100, line=dict(color='darkred', width=1.5, dash='dot'),name='MA100 5m', yaxis='y1')
               ]
    # 1h MAs
    MA_1h = [
        go.Scatter(x=sub_df1h.timestamp, y=sub_df1h.MA9, line=dict(color='blue', width=1.5), name='MA9 1h', yaxis='y1'),
        go.Scatter(x=sub_df1h.timestamp, y=sub_df1h.MA20, line=dict(color='darkorange', width=1.5), name='MA20 1h',yaxis='y1'),
        go.Scatter(x=sub_df1h.timestamp, y=sub_df1h.MA100, line=dict(color='darkred', width=1.5), name='MA100 1h',yaxis='y1'),
        ]
    # 1d MAs
    MA_1d = [
        go.Scatter(x=sub_df1d.timestamp, y=sub_df1d.MA9, line=dict(color='blue', width=1.5, dash='dash'), name='MA9 1d',yaxis='y1'),
        go.Scatter(x=sub_df1d.timestamp, y=sub_df1d.MA20, line=dict(color='darkorange', width=1.5, dash='dash'), name='MA20 1d', yaxis='y1'),
        go.Scatter(x=sub_df1d.timestamp, y=sub_df1d.MA100, line=dict(color='darkred', width=1.5, dash='dash'), name='MA100 1d', yaxis='y1')
    ]

    if base_df == '1d':
        #not plotting 5min MA
        MA_list = MA_1h + MA_1d
    else:
        MA_list = MA_5min + MA_1h + MA_1d

    start = pd.Timestamp('2020-11-03')
    end = pd.Timestamp('2021-12-25')
    t_ts = np.linspace(start.value, end.value, 100)
    t = pd.to_datetime(t_ts)
    t_df = pd.DataFrame(t)
    t_df.columns = ['Date_time']
    t_df['timestamp'] = t_df.Date_time.values.astype(np.int64) // 10 ** 9
    t_df.set_index(t_df['timestamp'], inplace=True)
    xx = np.asarray(t)
    xxx = []
    for x in xx:
        xxx.append(time.mktime(datetime.utcfromtimestamp(x.tolist() / 1e9).timetuple()))
    trends_plot = []
    for i, row in trends_df.iterrows():
        if 'up' in row['trend_name']:
            color = 'red'
        else:
            color = 'green'
        yy = row['slope'] * np.array(xxx) + row['interc']
        t_df[row['trend_name']] = yy
        trends_plot.append(go.Scatter(x=t_df['Date_time'], y=t_df[row['trend_name']], line=dict(color=color, width=1.5), name=row['trend_name']))
    data = [go.Candlestick(x=sub_df.timestamp,
                                         open=sub_df.open,
                                         high=sub_df.high,
                                         low=sub_df.low,
                                         close=sub_df.close,
                                         name='XBTUSD', yaxis='y1'),
                          ] + MA_list + trends_plot
    layout = go.Layout(
        xaxis=dict(
            rangeslider=dict(
                visible=False
            )
        )
    )



    fig = go.FigureWidget(data=data, layout=layout)
    fig.update_layout(xaxis_range=[sub_df.timestamp[0], sub_df.timestamp[-1]])

    if len(filled_df) != 0:

        filled_df['Date_time'] = pd.to_datetime(filled_df['timestamp'])
        # Plotting entered positions
        arrow_list = []
        for i, row in filled_df.iterrows():
           if row['side'] == 'Sell':
               color = 'red'
           else:
               color = 'green'
           arrow = dict(
                   x=row['Date_time'],
                   y=row['avgPx'],
                   xref="x", yref="y",
                   text=row['orderQty'],
                   showarrow=True,
                   axref="x", ayref='y',
                   ax=row['Date_time'],
                   ay=row['avgPx']-0.1*np.nanmax(sub_df['close']),
                   arrowhead=3,
                   arrowwidth=1.5,
                   arrowcolor=color, )

           arrow_list.append(arrow)

        fig.update_layout(annotations = arrow_list)
    if base_df == '1d':
        fig.update_layout(yaxis_range=[0, 1.10*np.nanmax(data_1d['close'])])
    return fig

#def wallet_plotter():
