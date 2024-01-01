from echarts import Echart, Legend, Line, Tooltip, Axis
from code.MySql.LoadMysql import StockPoolData

df = StockPoolData.load_poolCount()

print(df.tail(50))

print(df['_down'].tolist())
exit()

chart = Echart('My first chart', 'Profit and loss')
# chart.use(Tooltip('Up'))
# chart.use(Legend(['Profit', 'Loss']))
# chart.use(Axis('ReDown', 'Down', data=df['date'].tolist()))
# chart.use(Axis('ReDown', 'Down'))
chart.use(Line('date', df['_down'].tolist()))
# chart.use(Line('date', df['_BoardUp'].tolist()))
#
chart.plot()
