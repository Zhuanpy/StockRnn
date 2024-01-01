import pandas as pd

# 创建一个示例DataFrame
data = {'date': ['2023-01-01', '2023-01-02', '2023-01-03']}
df = pd.DataFrame(data)

# 将'date'列转换为datetime类型
df['date'] = pd.to_datetime(df['date'])
df['dates'] = df['date'].dt.date

# 打印转换后的DataFrame
print(df)