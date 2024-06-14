import json
import os
import pandas as pd

x = input("file path: ")

with open(x,"r") as f:
    raw_json = json.load(f)
key_list = list(raw_json.keys())
for i in key_list:
    save_json_file = pd.json_normalize(raw_json[i])
    save_json_file.to_json(f"split_clean_data_technical\\{i}", "records")

financialGrowth_df = pd.json_normalize(raw_json['financialGrowth'])
ratios_df = pd.json_normalize(raw_json['ratios'])
cashFlowStatementGrowth_df = pd.json_normalize(raw_json['cashFlowStatementGrowth'])
incomeStatementGrowth_df = pd.json_normalize(raw_json['incomeStatementGrowth'])
balanceSheetStatementGrowth_df = pd.json_normalize(raw_json['balanceSheetStatementGrowth'])
historicalPriceFull_df = pd.json_normalize(raw_json['historicalPriceFull']["historical"])
tech5_df = pd.json_normalize(raw_json['tech5'])
tech20_df = pd.json_normalize(raw_json['tech20'])
tech60_df = pd.json_normalize(raw_json['tech60'])
tech252_df = pd.json_normalize(raw_json['tech252'])

Growth_1011_TW_df = financialGrowth_df.merge(ratios_df,on=["date", "symbol", "calendarYear", "period"]).merge(cashFlowStatementGrowth_df,on=["date", "symbol", "calendarYear", "period"]).merge(incomeStatementGrowth_df,on=["date", "symbol", "calendarYear", "period"]).merge(balanceSheetStatementGrowth_df,on=["date", "symbol", "calendarYear", "period"])

def get_date_range(row):
    if row['period'] == 'Q1':
        return f"{row['calendarYear']}-01-01~{row['calendarYear']}-03-31"
    elif row['period'] == 'Q2':
        return f"{row['calendarYear']}-04-01~{row['calendarYear']}-06-30"
    elif row['period'] == 'Q3':
        return f"{row['calendarYear']}-07-01~{row['calendarYear']}-09-30"
    elif row['period'] == 'Q4':
        return f"{row['calendarYear']}-10-01~{row['calendarYear']}-12-31"
    else:
        return row['date']
    
def transfer_datetime_to_date(df):
    df['only_date'] = pd.to_datetime(df['date'])
    df = df.drop("date", axis=1)
    final_df = df.rename(columns={'only_date': 'date'})
    return final_df

def df_to_csv_and_save(df, filename):
    os.makedirs("csv", exist_ok= True)
    df.to_csv(f"csv\\{filename}.csv", index=False)


Growth_1011_TW_df['date_range'] = Growth_1011_TW_df.apply(get_date_range, axis=1)
Growth_1011_TW_df_final = Growth_1011_TW_df.drop("period", axis=1)

historicalPriceFull_df["symbol"] = "1101.TW"
historicalPriceFull_df


tech5_df_final = transfer_datetime_to_date(tech5_df)
tech20_df_final = transfer_datetime_to_date(tech20_df)
tech60_df_final = transfer_datetime_to_date(tech60_df)
tech252_df_final = transfer_datetime_to_date(tech252_df)



# tech5_df_final.to_csv("csv\\tech5_df_final.csv")

df_to_csv_and_save(Growth_1011_TW_df_final, "Growth_1011_TW_df_final")
df_to_csv_and_save(historicalPriceFull_df, "historicalPriceFull_df")
df_to_csv_and_save(tech5_df_final, "tech5_df_final")
df_to_csv_and_save(tech20_df_final, "tech20_df_final")
df_to_csv_and_save(tech60_df_final, "tech60_df_final")
df_to_csv_and_save(tech252_df_final, "tech252_df_final")