import pandas as pd

csv_McidFile = '../../GcpHde/Mcid/TlmMcidVxu20230926.csv'
csv_McidFile = '../../GcpHde/Mcid/TlmMcidAll20230927.csv'

df = pd.read_csv (csv_McidFile
                  ,index_col=0,header=2
                  ,sep='\t'
                  ,skip_blank_lines=True,skipfooter=1
                  ,skiprows=3)
print(df.head())

xl = pd.ExcelWriter('TlmMcidAll20230927.xlsx')
df.to_excel(xl,index=False)
xl.save()
