#%%
import pandas as pd
import numpy as np
import os
import shutil

#%%
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA.dta'

df = pd.read_stata(file_dir)

df = df.drop(df.loc[df['YYYY'] > "2018"].index)
df = df.drop(df.loc[df['YYYY'] < "2010"].index)

df
#%%
# long form을 wide로 변경하기!!
# 우선 기준값으로 index를 생성해 준다.
df['CON'] = df['CDW_ID'] + df['SM_DATE'] + df['RPTN_DT']
# df['idx'] = df.groupby('CDW_ID', 'SM_DATE', 'EXMN_CD').cumcount()+1
df['idx'] = df.groupby('CON').cumcount()+1
# 피봇 테이블을 만들어 준다.
df_wide = df.pivot_table(
                    index=['CDW_ID','SM_DATE','RPTN_DT'], columns='idx'
                   ,values=['HLSC_RSLT_CD'], aggfunc='first'
)

df_wide
#%%
# 정렬 한 후
df_wide = df_wide.sort_index(axis=1, level=1)
# 각 칼럼을 생성해 데이터를 채워준다.
df_wide.columns = [f'{x}_{y}' for x,y in df_wide.columns]
# 인덱스를 reset해 준다.
df_wide = df_wide.reset_index()

df_wide
 
# %%
df_wide.to_stata('H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta')

# %%
def target_img_copy(target_list,img_dir,save_dir):
    for target in target_list:
        yyyy = target[13:17]
        mm = target[17:19]
        dd = target[19:21]
        img_find_path = "{}/{}/{}{}".format(img_dir,yyyy,mm,dd)
        img_file_list = []
        for (root, dir, files) in os.walk(img_find_path):
            for file in files:
                if '.j' in file.lower():
                    file_dir = os.path.join(root, file)
                    img_file_list.append(file_dir)
        
        for img_file in img_file_list:        
            if target[:12] == img_file[27:39]:
                shutil.copy(img_file,save_dir+img_file[27:])

def grp_img_extract(df, rslt, save_dir):
    globals()['data_{}'.format(rslt)] = df.loc[df[rslt]==1]
    globals()['data_{}'.format(rslt)]['CON'] = globals()['data_{}'.format(rslt)]['CDW_ID'] + '_' + globals()['data_{}'.format(rslt)]['RPTN_DT']
    target_list = globals()['data_{}'.format(rslt)]['CON'].to_list()
    os.makedirs(save_dir)
    target_img_copy(target_list,img_dir,save_dir)
    globals()['data_{}'.format(rslt)].drop(['index','CON'],axis=1).to_excel(save_dir+'{}.xlsx'.format(rslt),index=False)

#%%
img_dir = "W:/안저 deID 이미지파일"

#%%
rslt = '맥락막'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R031') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '고혈압성 망막증'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L021') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R021') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '망막박리'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L033') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R033') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)
                
#%%
rslt = '시신경'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L026') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O026') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R026') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R036') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R801') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '고도근시'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O027') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R027') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O031') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = 'macular hole'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L024') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R024') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O035') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '매체혼탁'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O014') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '정맥폐쇄'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L011') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L013') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O011') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R011') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '당뇨병성 망막증'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L022') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O030') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R020') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R022') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '망막'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L017') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O017') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O037') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R017') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R035') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '황반'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L019') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O019') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R019') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '망막 변성'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L018') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O018') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R018') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '출혈'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

data = pd.read_stata(file_dir)

data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L030') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O036') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R030') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

# #%%
# rslt = '드루젠'
# file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_1018.dta'

# data = pd.read_stata(file_dir)

# data[rslt] = ((data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L701') |
#               (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O701') |
#               (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R701') 
#              ).any(axis=1).astype(int)

# save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

# grp_img_extract(data, rslt, save_dir)

#%%
## 여기서 부터는 결과 숫자가 많아서 양안인 결과 대상자만 이미지 추출 진행
rslt = '드루젠'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_2018.dta'

data = pd.read_stata(file_dir)

data[rslt] = (#(data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L701') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O701')# |
              #(data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R701') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '망막전막'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_2018.dta'

data = pd.read_stata(file_dir)

data[rslt] = (#(data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L701') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O016')# |
              #(data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R701') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)

#%%
rslt = '황반변성'
file_dir = 'H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/DATA_WIDE_2018.dta'

data = pd.read_stata(file_dir)

data[rslt] = (#(data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='L701') |
              (data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='O034')# |
              #(data.loc[:,'HLSC_RSLT_CD_1':'HLSC_RSLT_CD_6']=='R701') 
             ).any(axis=1).astype(int)

save_dir = "H:/업무/자료요청/2022년/DATA클리닝/강미라_220516_뷰노AI/결과별 안저촬영 IMAGE DATA/{}/".format(rslt)

grp_img_extract(data, rslt, save_dir)
# %%
