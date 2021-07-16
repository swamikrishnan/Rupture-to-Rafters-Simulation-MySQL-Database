import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
# import mysql.connector as msql
# from mysql.connector import Error
from getpass import getpass
import csv
# def connect(conn_params_dic):
#    conn = None
#    try:
#        print('Connecting to the MySQL database...')
#        conn = msql.connect(**conn_params_dic)
#        print('Connection successful')
#    except Error as err:
#        print('Error while connecting to MySQL',err)
#        conn = None
#    return conn
conn_params_dic = {
    "host"      :   "localhost",
    "database"  :   "rupture_to_rafters_db",
    "user"      :   input("Enter username for the MySQL database"),
    "password"  :   getpass("Enter password: ")
}
# Define function to connect to MySQL database using sqlalchemy with pymysql dialect
connect_alchemy = "mysql+pymysql://%s:%s@%s/%s" % (
    conn_params_dic['user'],
    conn_params_dic['password'],
    conn_params_dic['host'],
    conn_params_dic['database']
)
def using_alchemy():
    try:
        print('Connecting to the MySQL database...')
        engine = create_engine(connect_alchemy)
        print("Connected successfully...")
    except SQLAlchemyError as e:
        err=str(e._dic_['orig'])
        print("Error while connecting to MySQL", err)
        # set the connection to 'None' in case of error
        engine = None
    return engine
# Connect to the MySQL database
mysql_engine = using_alchemy()
# Create table
# tab_cols: list of lists: [[col_name, sql_var_type, NULL/NOT NULL], [], []...]

def create_table(engine,schema_name,tab_name,tab_cols):
    sql_comm = "DROP TABLE IF EXISTS " + schema_name + "." + tab_name + ";"
    engine.execute(sql_comm)
    tab_cols_def_string = ','.join(item for item in tab_cols)
    sql_comm = "CREATE TABLE " + schema_name + "." + tab_name + "(" + tab_cols_def_string +")"
    print("Executing the following MySQL command...")
    print(sql_comm)
    engine.execute(sql_comm)
    print("Table ", tab_name, " created successfully")
#
# Use the code below to create a new table in the MySQL database
exec_flag = 0
while exec_flag==1:
    tab_name = '07_f3d_RTH'
    tab_cols = ["EARTHQUAKE VARCHAR(45) NOT NULL", "STATION VARCHAR(45) NOT NULL", "BUILDING VARCHAR(45) NOT NULL", "TIME DOUBLE NOT NULL",
                "R001 DOUBLE NULL", "R002 DOUBLE NULL", "R003 DOUBLE NULL", "R004 DOUBLE NULL", "R005 DOUBLE NULL", "R006 DOUBLE NULL", "R007 DOUBLE NULL", "R008 DOUBLE NULL", "R009 DOUBLE NULL", "R010 DOUBLE NULL",
                "R011 DOUBLE NULL", "R012 DOUBLE NULL", "R013 DOUBLE NULL", "R014 DOUBLE NULL", "R015 DOUBLE NULL", "R016 DOUBLE NULL", "R017 DOUBLE NULL", "R018 DOUBLE NULL", "R019 DOUBLE NULL", "R020 DOUBLE NULL",
                "R021 DOUBLE NULL", "R022 DOUBLE NULL", "R023 DOUBLE NULL", "R024 DOUBLE NULL", "R025 DOUBLE NULL", "R026 DOUBLE NULL", "R027 DOUBLE NULL", "R028 DOUBLE NULL", "R029 DOUBLE NULL", "R030 DOUBLE NULL",
                "R031 DOUBLE NULL", "R032 DOUBLE NULL", "R033 DOUBLE NULL", "R034 DOUBLE NULL", "R035 DOUBLE NULL", "R036 DOUBLE NULL", "R037 DOUBLE NULL", "R038 DOUBLE NULL", "R039 DOUBLE NULL", "R040 DOUBLE NULL",
                "R041 DOUBLE NULL", "R042 DOUBLE NULL", "R043 DOUBLE NULL", "R044 DOUBLE NULL", "R045 DOUBLE NULL", "R046 DOUBLE NULL", "R047 DOUBLE NULL", "R048 DOUBLE NULL", "R049 DOUBLE NULL", "R050 DOUBLE NULL",
                "R051 DOUBLE NULL", "R052 DOUBLE NULL", "R053 DOUBLE NULL", "R054 DOUBLE NULL", "R055 DOUBLE NULL", "R056 DOUBLE NULL", "R057 DOUBLE NULL", "R058 DOUBLE NULL", "R059 DOUBLE NULL", "R060 DOUBLE NULL",
                "R061 DOUBLE NULL", "R062 DOUBLE NULL", "R063 DOUBLE NULL", "R064 DOUBLE NULL", "R065 DOUBLE NULL", "R066 DOUBLE NULL", "R067 DOUBLE NULL", "R068 DOUBLE NULL", "R069 DOUBLE NULL", "R070 DOUBLE NULL",
                "R071 DOUBLE NULL", "R072 DOUBLE NULL", "R073 DOUBLE NULL", "R074 DOUBLE NULL", "R075 DOUBLE NULL", "R076 DOUBLE NULL", "R077 DOUBLE NULL", "R078 DOUBLE NULL", "R079 DOUBLE NULL", "R080 DOUBLE NULL",
                "R081 DOUBLE NULL", "R082 DOUBLE NULL", "R083 DOUBLE NULL", "R084 DOUBLE NULL", "R085 DOUBLE NULL", "R086 DOUBLE NULL", "R087 DOUBLE NULL", "R088 DOUBLE NULL", "R089 DOUBLE NULL", "R090 DOUBLE NULL",
                "R091 DOUBLE NULL", "R092 DOUBLE NULL", "R093 DOUBLE NULL", "R094 DOUBLE NULL", "R095 DOUBLE NULL", "R096 DOUBLE NULL", "R097 DOUBLE NULL", "R098 DOUBLE NULL", "R099 DOUBLE NULL", "R100 DOUBLE NULL",
                "R101 DOUBLE NULL", "R102 DOUBLE NULL", "R103 DOUBLE NULL", "R104 DOUBLE NULL", "R105 DOUBLE NULL", "R106 DOUBLE NULL", "R107 DOUBLE NULL", "R108 DOUBLE NULL", "R109 DOUBLE NULL", "R110 DOUBLE NULL",
                "R111 DOUBLE NULL", "R112 DOUBLE NULL", "R113 DOUBLE NULL", "R114 DOUBLE NULL", "R115 DOUBLE NULL", "R116 DOUBLE NULL", "R117 DOUBLE NULL", "R118 DOUBLE NULL", "R119 DOUBLE NULL", "R120 DOUBLE NULL",
                "R121 DOUBLE NULL", "R122 DOUBLE NULL", "R123 DOUBLE NULL", "R124 DOUBLE NULL", "R125 DOUBLE NULL", "R126 DOUBLE NULL", "R127 DOUBLE NULL", "R128 DOUBLE NULL", "R129 DOUBLE NULL", "R130 DOUBLE NULL",
                "R131 DOUBLE NULL", "R132 DOUBLE NULL", "R133 DOUBLE NULL", "R134 DOUBLE NULL", "R135 DOUBLE NULL", "R136 DOUBLE NULL", "R137 DOUBLE NULL", "R138 DOUBLE NULL", "R139 DOUBLE NULL", "R140 DOUBLE NULL",
                "R141 DOUBLE NULL", "R142 DOUBLE NULL", "R143 DOUBLE NULL", "R144 DOUBLE NULL", "R145 DOUBLE NULL", "R146 DOUBLE NULL", "R147 DOUBLE NULL", "R148 DOUBLE NULL", "R149 DOUBLE NULL", "R150 DOUBLE NULL",
                "R151 DOUBLE NULL", "R152 DOUBLE NULL", "R153 DOUBLE NULL", "R154 DOUBLE NULL", "R155 DOUBLE NULL", "R156 DOUBLE NULL", "R157 DOUBLE NULL", "R158 DOUBLE NULL", "R159 DOUBLE NULL", "R160 DOUBLE NULL",
                "R161 DOUBLE NULL", "R162 DOUBLE NULL", "R163 DOUBLE NULL", "R164 DOUBLE NULL", "R165 DOUBLE NULL", "R166 DOUBLE NULL", "R167 DOUBLE NULL", "R168 DOUBLE NULL", "R169 DOUBLE NULL", "R170 DOUBLE NULL",
                "R171 DOUBLE NULL", "R172 DOUBLE NULL", "R173 DOUBLE NULL", "R174 DOUBLE NULL", "R175 DOUBLE NULL", "R176 DOUBLE NULL", "R177 DOUBLE NULL", "R178 DOUBLE NULL", "R179 DOUBLE NULL", "R180 DOUBLE NULL",
                "R181 DOUBLE NULL", "R182 DOUBLE NULL", "R183 DOUBLE NULL", "R184 DOUBLE NULL", "R185 DOUBLE NULL", "R186 DOUBLE NULL", "R187 DOUBLE NULL", "R188 DOUBLE NULL", "R189 DOUBLE NULL", "R190 DOUBLE NULL",
                "R191 DOUBLE NULL", "R192 DOUBLE NULL", "R193 DOUBLE NULL", "R194 DOUBLE NULL", "R195 DOUBLE NULL", "R196 DOUBLE NULL", "R197 DOUBLE NULL", "R198 DOUBLE NULL", "R199 DOUBLE NULL", "R200 DOUBLE NULL",
                "R201 DOUBLE NULL", "R202 DOUBLE NULL", "R203 DOUBLE NULL", "R204 DOUBLE NULL", "R205 DOUBLE NULL", "R206 DOUBLE NULL", "R207 DOUBLE NULL", "R208 DOUBLE NULL", "R209 DOUBLE NULL", "R210 DOUBLE NULL",
                "R211 DOUBLE NULL", "R212 DOUBLE NULL", "R213 DOUBLE NULL", "R214 DOUBLE NULL", "R215 DOUBLE NULL", "R216 DOUBLE NULL", "R217 DOUBLE NULL", "R218 DOUBLE NULL", "R219 DOUBLE NULL", "R220 DOUBLE NULL",
                "R221 DOUBLE NULL", "R222 DOUBLE NULL", "R223 DOUBLE NULL", "R224 DOUBLE NULL", "R225 DOUBLE NULL", "R226 DOUBLE NULL", "R227 DOUBLE NULL", "R228 DOUBLE NULL", "R229 DOUBLE NULL", "R230 DOUBLE NULL",
                "R231 DOUBLE NULL", "R232 DOUBLE NULL", "R233 DOUBLE NULL", "R234 DOUBLE NULL", "R235 DOUBLE NULL", "R236 DOUBLE NULL", "R237 DOUBLE NULL", "R238 DOUBLE NULL", "R239 DOUBLE NULL", "R240 DOUBLE NULL",
                "R241 DOUBLE NULL", "R242 DOUBLE NULL", "R243 DOUBLE NULL", "R244 DOUBLE NULL", "R245 DOUBLE NULL", "R246 DOUBLE NULL", "R247 DOUBLE NULL", "R248 DOUBLE NULL", "R249 DOUBLE NULL", "R250 DOUBLE NULL",
                ]
    create_table(mysql_engine,"rupture_to_rafters_db",tab_name,tab_cols)
    exec_flag = 0
#
# Function to insert data into a table in a MySQL database
# tab_data is a pandas dataframe to be inserted into MySQL database.
# Dataframe column labels should match those of the MySQL table.
# Missing columns in the data OK.
def add_data_to_table(engine,schema_name,tab_name,tab_data,append_replace):
    try:
        tab_data.to_sql(tab_name,con=engine,schema=schema_name,if_exists=append_replace,index=False)
        print("Data inserted successfully...")
    except Error as err:
        print("Error while inserting to MySQL", e)
# INGESTING TEST DATA
# file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
# header_list = ['STATION']
# stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list)
# header_list = ['COL1', 'COL2']
# test_data_df = pd.read_csv(file_path+"/"+stations_df.iloc[0].to_string(index=False)+".test",sep=None,names=header_list,engine='python')
# Ingesting references into the "01_references" table of the rupture_to_rafters_db schema
exec_flag = 0
while exec_flag==1:
    print("INGESTING REFERENCES INTO TABLE 01_REFERENCES")
    file_path = "../../vm-shared/references"
    header_list = ['REF_ID','AUTHORS','YEAR','TITLE','PUBLICATION','VOLUME','NUMBER','PAGES','URL','DOI']
    ref_df = pd.read_csv(file_path+"/ref_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "01_references", ref_df,"append")
#    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "01_references", ref_df,"replace")
    exec_flag = 0
# Ingesting earthquake details into the "02_eqkes" table of the rupture_to_rafters_db schema
exec_flag = 0
while exec_flag==1:
    print("INGESTING EQKE DETAILS INTO TABLE 02_EQKES")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "02_eqkes", eqke_df,"append")
#    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "02_eqkes", eqke_df,"replace")
    exec_flag = 0
# Ingesting station info into the "03_stations" table of the rupture_to_rafters_db schema
exec_flag = 0
while exec_flag==1:
    print("INGESTING STATION INFO INTO TABLE 03_STATIONS")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION','LATITUDE','LONGITUDE']
    stations_db_df = pd.read_csv(file_path+"/stations_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "03_stations", stations_db_df,"append")
#    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "03_stations", stations_db_df,"replace")
    exec_flag = 0
# Ingesting ground motion data in a loop into the "04_gmhist" table of the rupture_to_rafters_db schema
exec_flag = 0
while exec_flag==1:
    print("INGESTING SPECFEM GROUND MOTION INTO TABLE 04_GMHIST")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/SESAME_SEM"
    header_list = ['EARTHQUAKE','STATION','TIME','ACCX','ACCY','ACCZ','VELX','VELY','VELZ','DISX','DISY','DISZ']
    header_list_accx = ['TIME','ACCX']
    header_list_accy = ['TIME','ACCY']
    header_list_accz = ['TIME','ACCZ']
    header_list_velx = ['TIME','VELX']
    header_list_vely = ['TIME','VELY']
    header_list_velz = ['TIME','VELZ']
    header_list_disx = ['TIME','DISX']
    header_list_disy = ['TIME','DISY']
    header_list_disz = ['TIME','DISZ']
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in stations_df["STATION"]:
        gm_accx_df = pd.read_csv(file_path+"/"+item+".SW.HXE.sema",names=header_list_accx,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_accy_df = pd.read_csv(file_path+"/"+item+".SW.HXN.sema",names=header_list_accy,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_accz_df = pd.read_csv(file_path+"/"+item+".SW.HXZ.sema",names=header_list_accz,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_velx_df = pd.read_csv(file_path+"/"+item+".SW.HXE.semv",names=header_list_velx,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_vely_df = pd.read_csv(file_path+"/"+item+".SW.HXN.semv",names=header_list_vely,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_velz_df = pd.read_csv(file_path+"/"+item+".SW.HXZ.semv",names=header_list_velz,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_disx_df = pd.read_csv(file_path+"/"+item+".SW.HXE.semd",names=header_list_disx,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_disy_df = pd.read_csv(file_path+"/"+item+".SW.HXN.semd",names=header_list_disy,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_disz_df = pd.read_csv(file_path+"/"+item+".SW.HXZ.semd",names=header_list_disz,skiprows=0,skipinitialspace=True,sep='\s+')
        if count==0:
# create a one column dataframe with the name of eqke
            eqke_df_full = pd.DataFrame(index=range(len(gm_accx_df)),columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
        stations_df_full = pd.DataFrame(index=range(len(gm_accx_df)),columns=['STATION'])
        stations_df_full['STATION'] = item
        gm_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], gm_accx_df['TIME'],
                               gm_accx_df['ACCX'], gm_accy_df['ACCY'], gm_accz_df['ACCZ'],
                               gm_velx_df['VELX'], gm_vely_df['VELY'], gm_velz_df['VELZ'],
                               gm_disx_df['DISX'], gm_disy_df['DISY'], gm_disz_df['DISZ']],
                               axis=1,keys=header_list)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","04_gmhist",gm_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","04_gmhist",gm_tab_df,"replace")
        print("Record: ",count,"  Station: ",item)
    exec_flag = 0
# Ingesting FRAME3D for090, for091, for092 data in a loop into the "05_f3d_AVD" table of the rupture_to_rafters_db schema
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D FOR090, FOR091, FOR092 FROM AVDDIR INTO TABLE 05_F3D_AVD")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
    header_list = ['EARTHQUAKE','STATION','TIME','ACCX','ACCY','ACCZ','VELX','VELY','VELZ','DISX','DISY','DISZ']
    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    header_list_avdy = ['TIME','ACCY','VELY','DISY']
    header_list_avdz = ['TIME','ACCZ','VELZ','DISZ']
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in stations_df["STATION"]:
        gm_avdx_df = pd.read_csv(file_path+"/"+item+".HXE.avdx",names=header_list_avdx,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_avdy_df = pd.read_csv(file_path+"/"+item+".HXN.avdy",names=header_list_avdy,skiprows=0,skipinitialspace=True,sep='\s+')
        gm_avdz_df = pd.read_csv(file_path+"/"+item+".HXZ.avdz",names=header_list_avdz,skiprows=0,skipinitialspace=True,sep='\s+')
# create a one column dataframe with the name of the eqke
        eqke_df_full = pd.DataFrame(index=range(len(gm_avdx_df)),columns=['EARTHQUAKE'])
        eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
        stations_df_full = pd.DataFrame(index=range(len(gm_avdx_df)),columns=['STATION'])
        stations_df_full['STATION'] = item
        f3d_gm_avd_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], gm_avdx_df['TIME'],
                                       gm_avdx_df['ACCX'], gm_avdy_df['ACCY'], gm_avdz_df['ACCZ'],
                                       gm_avdx_df['VELX'], gm_avdy_df['VELY'], gm_avdz_df['VELZ'],
                                       gm_avdx_df['DISX'], gm_avdy_df['DISY'], gm_avdz_df['DISZ']],
                                       axis=1,keys=header_list)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","05_f3d_AVD",f3d_gm_avd_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","05_f3d_AVD",f3d_gm_avd_tab_df,"replace")
        print("Record: ",count,"  Station: ",item)
    exec_flag = 0

# Ingesting building details into the "06_bldgs" table of the rupture_to_rafters_db schema
exec_flag = 0
while exec_flag==1:
    print("INGESTING BLDG DETAILS INTO TABLE 06_BLDGS")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "06_bldgs", bldg_df,"append")
#    add_data_to_table(mysql_engine, "rupture_to_rafters_db", "06_bldgs", bldg_df,"replace")
    exec_flag = 0

# Ingesting FRAME3D COORD file into the "07_f3d_COORD" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D COORD FILE INTO TABLE 07_F3D_COORD")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['NODE','X','Y','Z','ATTPT1_X','ATTPT1_Y','ATTPT1_Z',
                   'ATTPT2_X','ATTPT2_Y','ATTPT2_Z','ATTPT3_X','ATTPT3_Y','ATTPT3_Z',
                   'ATTPT4_X','ATTPT4_Y','ATTPT4_Z','ATTPT5_X','ATTPT5_Y','ATTPT5_Z',
                   'ATTPT6_X','ATTPT6_Y','ATTPT6_Z'
                   ]
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist/output"
    f3d_coord_df = pd.read_csv(file_path+"/"+stations_df.loc[:,'STATION'][0]+"/COORD",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_coord_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_coord_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_coord_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","07_f3d_COORD",f3d_coord_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","07_f3d_COORD",f3d_coord_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D BEAM file into the "09_f3d_BEAM" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D BEAM FILE INTO TABLE 09_F3D_BEAM")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['ELEM','NODE1','NODE2','N1ATTACH','N2ATTACH','IELTYPE']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist/output"
    f3d_beam_df = pd.read_csv(file_path+"/"+stations_df.loc[:,'STATION'][0]+"/BEAM",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_beam_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_beam_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_beam_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","09_f3d_BEAM",f3d_beam_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","09_f3d_BEAM",f3d_beam_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D PLSTR file into the "10_f3d_PLSTR" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D BEAM FILE INTO TABLE 10_F3D_PLSTR")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['ELEM','NODE1','NODE2','NODE3','NODE4']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist/output"
    f3d_plstr_df = pd.read_csv(file_path+"/"+stations_df.loc[:,'STATION'][0]+"/PLSTR",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_plstr_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_plstr_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_plstr_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","10_f3d_PLSTR",f3d_plstr_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","10_f3d_PLSTR",f3d_plstr_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D BUCKLING file into the "11_f3d_BUCKLING" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D BUCKLING FILE INTO TABLE 11_F3D_BUCKLING")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['ELEM','X1','Y1','Z1','X2','Y2','Z2']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist/output"
    f3d_buckling_df = pd.read_csv(file_path+"/"+stations_df.loc[:,'STATION'][0]+"/BUCKLING",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_buckling_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_buckling_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_buckling_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","11_f3d_BUCKLING",f3d_buckling_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","11_f3d_BUCKLING",f3d_buckling_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D EIGEN files into the "12_f3d_EIGEN" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D EIGEN FILE INTO TABLE 12_F3D_EIGEN")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    count = 0
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
        header_list = ['T01','T02','T03','T04','T05','T06','T07','T08','T09','T10','T11','T12','T13','T14','T15',
                       'T16','T17','T18','T19','T20','T21','T22','T23','T24','T25','T26','T27','T28','T29','T30']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        f3d_eigen_df = pd.read_csv(file_path+"/"+bldgs[count]+"/EIGEN",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
        header_list_tab = ['BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_eigen_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_eigen_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_eigen_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","12_f3d_EIGEN",f3d_eigen_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","12_f3d_EIGEN",f3d_eigen_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D MODES files into the "13_f3d_MODES" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D MODES FILE INTO TABLE 13_F3D_MODES")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    count = 0
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
        header_list = ['DOF','MODE01','MODE02','MODE03','MODE04','MODE05','MODE06','MODE07','MODE08','MODE09','MODE10',
                       'MODE11','MODE12','MODE13','MODE14','MODE15','MODE16','MODE17','MODE18','MODE19','MODE20',
                       'MODE21','MODE22','MODE23','MODE24','MODE25','MODE26','MODE27','MODE28','MODE29','MODE30']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        f3d_modes_df = pd.read_csv(file_path+"/"+bldgs[count]+"/MODES",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
        header_list_tab = ['BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_modes_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_modes_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_modes_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","13_f3d_MODES",f3d_modes_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","13_f3d_MODES",f3d_modes_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D XDRFT_RTH file into the "14_f3d_XDRFT_ID" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D XDRFT_ID FILE INTO TABLE 14_F3D_XDRFT_ID")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['NODE1','NODE2','STORY_HT','XDRFT_ID']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist"
    f3d_xdrft_id_df = pd.read_csv(file_path+"/XDRFT_ID",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_xdrft_id_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_xdrft_id_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_xdrft_id_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","14_f3d_XDRFT_ID",f3d_xdrft_id_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","14_f3d_XDRFT_ID",f3d_xdrft_id_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D YDRFT_RTH file into the "15_f3d_YDRFT_ID" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D XDRFT_ID FILE INTO TABLE 15_F3D_YDRFT_ID")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['NODE1','NODE2','STORY_HT','YDRFT_ID']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist"
    f3d_ydrft_id_df = pd.read_csv(file_path+"/YDRFT_ID",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_ydrft_id_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_ydrft_id_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_ydrft_id_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","15_f3d_YDRFT_ID",f3d_ydrft_id_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","15_f3d_YDRFT_ID",f3d_ydrft_id_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D ELMGRPRES_ID file into the "16_f3d_ELMGRPRES_ID" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D ELMGRPRES_ID FILE INTO TABLE 16_f3d_ELMGRPRES_ID")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['E01','E01_N12','E02','E02_N12','E03','E03_N12','E04','E04_N12','E05','E05_N12','E06','E06_N12',
                   'E07','E07_N12','E08','E08_N12','E09','E09_N12','E10','E10_N12','E11','E11_N12','E12','E12_N12',
                   'E13','E13_N12','E14','E14_N12','E15','E15_N12','E16','E16_N12','E17','E17_N12','E18','E18_N12',
                   'E19','E19_N12','E20','E20_N12','E21','E21_N12','E22','E22_N12','E23','E23_N12','E24','E24_N12',
                   'E25','E25_N12','E26','E26_N12','E27','E27_N12','E28','E28_N12','E29','E29_N12','E30','E30_N12',
                   'E31','E31_N12','E32','E32_N12','E33','E33_N12','E34','E34_N12','E35','E35_N12','E36','E36_N12',
                   'E37','E37_N12','E38','E38_N12','E39','E39_N12','E40','E40_N12']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist"
    f3d_elmgrpres_id_df = pd.read_csv(file_path+"/ELMGRPRES_ID",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
    header_list_tab = ['BUILDING','GRP_ID'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_elmgrpres_id_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        grp_df = pd.DataFrame(index=range(len(f3d_elmgrpres_id_df)),columns=['GRP_ID'],data=range(1,len(f3d_elmgrpres_id_df)+1))
        f3d_elmgrpres_id_tab_df = pd.concat([bldg_df_full['BUILDING'], grp_df['GRP_ID'], f3d_elmgrpres_id_df], axis = 1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","16_f3d_ELMGRPRES_ID",f3d_elmgrpres_id_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","16_f3d_ELMGRPRES_ID",f3d_elmgrpres_id_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D RTH-ID file into the "17_f3d_RTH_ID" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile for the earthquake listed in the eqke_dbfile.csv file
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D RTH-ID FILE INTO TABLE 17_F3D_RTH_ID")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['NODE','NODE_RESPID','ELEM','ELEM_RESPID','PANELZONE','PZ_RESPID','ELEMENT','SEGMENT','FIBER','FIBER_RESPID','RTH_ID']
    f3d_rth_id_df = pd.read_csv(file_path+"/bldg_rth_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
    header_list = ['BUILDING'] + header_list
    count = 0
# Whitespace delimiter: sep='\s+'
    for item in bldg_df["BUILDING"]:
# create a one column dataframe with the name of the bldg
        bldg_df_full = pd.DataFrame(index=range(len(f3d_rth_id_df)),columns=['BUILDING'])
        bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
        f3d_rth_id_tab_df = pd.concat([bldg_df_full['BUILDING'], f3d_rth_id_df], axis=1)
        count+=1
        add_data_to_table(mysql_engine,"rupture_to_rafters_db","17_f3d_RTH_ID",f3d_rth_id_tab_df,"append")
#        add_data_to_table(mysql_engine,"rupture_to_rafters_db","17_f3d_RTH_ID",f3d_rth_id_tab_df,"replace")
        print("Bldg#: ",count,"  Bldg: ",item)
    exec_flag = 0

# Ingesting FRAME3D RTH files into the "18_f3d_RTH" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D RTH FILE INTO TABLE 18_F3D_RTH")
    file_path_avd = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['R001', 'R002', 'R003', 'R004', 'R005', 'R006', 'R007', 'R008', 'R009', 'R010',
                        'R011', 'R012', 'R013', 'R014', 'R015', 'R016', 'R017', 'R018', 'R019', 'R020',
                        'R021', 'R022', 'R023', 'R024', 'R025', 'R026', 'R027', 'R028', 'R029', 'R030',
                        'R031', 'R032', 'R033', 'R034', 'R035', 'R036', 'R037', 'R038', 'R039', 'R040',
                        'R041', 'R042', 'R043', 'R044', 'R045', 'R046', 'R047', 'R048', 'R049', 'R050',
                        'R051', 'R052', 'R053', 'R054', 'R055', 'R056', 'R057', 'R058', 'R059', 'R060',
                        'R061', 'R062', 'R063', 'R064', 'R065', 'R066', 'R067', 'R068', 'R069', 'R070',
                        'R071', 'R072', 'R073', 'R074', 'R075', 'R076', 'R077', 'R078', 'R079', 'R080',
                        'R081', 'R082', 'R083', 'R084', 'R085', 'R086', 'R087', 'R088', 'R089', 'R090',
                        'R091', 'R092', 'R093', 'R094', 'R095', 'R096', 'R097', 'R098', 'R099', 'R100',
                        'R101', 'R102', 'R103', 'R104', 'R105', 'R106', 'R107', 'R108', 'R109', 'R110',
                        'R111', 'R112', 'R113', 'R114', 'R115', 'R116', 'R117', 'R118', 'R119', 'R120',
                        'R121', 'R122', 'R123', 'R124', 'R125', 'R126', 'R127', 'R128', 'R129', 'R130',
                        'R131', 'R132', 'R133', 'R134', 'R135', 'R136', 'R137', 'R138', 'R139', 'R140',
                        'R141', 'R142', 'R143', 'R144', 'R145', 'R146', 'R147', 'R148', 'R149', 'R150',
                        'R151', 'R152', 'R153', 'R154', 'R155', 'R156', 'R157', 'R158', 'R159', 'R160',
                        'R161', 'R162', 'R163', 'R164', 'R165', 'R166', 'R167', 'R168', 'R169', 'R170',
                        'R171', 'R172', 'R173', 'R174', 'R175', 'R176', 'R177', 'R178', 'R179', 'R180',
                        'R181', 'R182', 'R183', 'R184', 'R185', 'R186', 'R187', 'R188', 'R189', 'R190',
                        'R191', 'R192', 'R193', 'R194', 'R195', 'R196', 'R197', 'R198', 'R199', 'R200',
                        'R201', 'R202', 'R203', 'R204', 'R205', 'R206', 'R207', 'R208', 'R209', 'R210',
                        'R211', 'R212', 'R213', 'R214', 'R215', 'R216', 'R217', 'R218', 'R219', 'R220',
                        'R221', 'R222', 'R223', 'R224', 'R225', 'R226', 'R227', 'R228', 'R229', 'R230',
                        'R231', 'R232', 'R233', 'R234', 'R235', 'R236', 'R237', 'R238', 'R239', 'R240',
                        'R241', 'R242', 'R243', 'R244', 'R245', 'R246', 'R247', 'R248', 'R249', 'R250'
                        ]
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            gm_avdx_df = pd.read_csv(file_path_avd + "/" + item2 + ".HXE.avdx", names=header_list_avdx, skiprows=0,
                                     skipinitialspace=True, sep='\s+')
            f3d_rth_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/RTH",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING','TIME'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_rth_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_rth_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_rth_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_rth_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], gm_avdx_df['TIME'], f3d_rth_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","18_f3d_RTH",f3d_rth_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","18_f3d_RTH",f3d_rth_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D BMPLROT files into the "19_f3d_BMPLROT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D BMPLROT FILE INTO TABLE 19_F3D_BMPLROT")
#    file_path_avd = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
#    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['ELEM','X-N1ATTACH','Y-N1ATTACH','Z-N1ATTACH','YROT1','ZROT1','X-N2ATTACH','Y-N2ATTACH','Z-N2ATTACH','YROT2','ZROT2']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
#            gm_avdx_df = pd.read_csv(file_path_avd + "/" + item2 + ".HXE.avdx", names=header_list_avdx, skiprows=0,
#                                     skipinitialspace=True, sep='\s+')
            f3d_bmplrot_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/BMPLROT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_bmplrot_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_bmplrot_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_bmplrot_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_bmplrot_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_bmplrot_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","19_f3d_BMPLROT",f3d_bmplrot_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","19_f3d_BMPLROT",f3d_bmplrot_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D PZPLROT files into the "20_f3d_PZPLROT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D PZPLROT FILE INTO TABLE 20_F3D_PZPLROT")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['NODE','X','Y','Z','YRPZ','ZRPZ']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_pzplrot_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/PZPLROT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_pzplrot_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_pzplrot_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_pzplrot_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_pzplrot_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_pzplrot_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","20_f3d_PZPLROT",f3d_pzplrot_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","20_f3d_PZPLROT",f3d_pzplrot_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D PLSTRSS files into the "21_f3d_PLSTRSS" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D PLSTRSS FILE INTO TABLE 21_F3D_PLSTRSS")
    file_path_avd = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['PLSTRELEM','SIGMA1','SIGMA2','THETA1','TAUMAX']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            gm_avdx_df = pd.read_csv(file_path_avd + "/" + item2 + ".HXE.avdx", names=header_list_avdx, skiprows=0,
                                     skipinitialspace=True, sep='\s+')
            f3d_plstrss_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/PLSTRSS",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            nplstrss = len(f3d_plstrss_df)//(len(gm_avdx_df)+1)
            f3d_plstrss_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/PLSTRSS",names=header_list,skiprows=nplstrss,skipinitialspace=True,sep='\s+')
            time_full = pd.DataFrame(columns=['TIME'])
            time_short = pd.DataFrame(index=range(0,nplstrss), columns=['TIME'])
            if nplstrss!= 0:
                for i in range(len(gm_avdx_df)):
                    time_short['TIME'] = gm_avdx_df.iloc[i,0]
                    time_full = time_full.append(time_short,ignore_index=True)
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING','TIME'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_plstrss_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_plstrss_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_plstrss_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_plstrss_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], time_full['TIME'], f3d_plstrss_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","21_f3d_PLSTRSS",f3d_plstrss_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","21_f3d_PLSTRSS",f3d_plstrss_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D PZPLROT files into the "22_f3d_PLSTRSMAX" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D PLSTRSMAX FILE INTO TABLE 22_F3D_PLSTRSMAX")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['PLSTRELEM','SIGMA1MAX','THETA1MAX','SIGMA2MIN','THETA2MIN','TAUMAX']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_plstrsmax_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/PLSTRSMAX",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_plstrsmax_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_plstrsmax_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_plstrsmax_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_plstrsmax_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_plstrsmax_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","22_f3d_PLSTRSMAX",f3d_plstrsmax_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","22_f3d_PLSTRSMAX",f3d_plstrsmax_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D FRAC files into the "23_f3d_FRAC" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D FRAC FILE INTO TABLE 23_F3D_FRAC")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['TIME','ELEM','SEGM','FIBER']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_frac_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/FRAC",names=header_list,skiprows=2,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_frac_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_frac_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_frac_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_frac_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_frac_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","23_f3d_FRAC",f3d_frac_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","23_f3d_FRAC",f3d_frac_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D FRACSUM files into the "24_f3d_FRACSUM" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D FRACSUM FILE INTO TABLE 24_F3D_FRACSUM")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['NUMBER','ELEM','SEGM']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_fracsum_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/FRACSUM",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_fracsum_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_fracsum_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_fracsum_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_fracsum_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_fracsum_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","24_f3d_FRACSUM",f3d_fracsum_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","24_f3d_FRACSUM",f3d_fracsum_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D FRACTOT files into the "25_f3d_FRACTOT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D FRACTOT FILE INTO TABLE 25_F3D_FRACTOT")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['TOT_NUM_OF_FRAC']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_fractot_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/FRACTOT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_fractot_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_fractot_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_fractot_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_fractot_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_fractot_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","25_f3d_FRACTOT",f3d_fractot_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","25_f3d_FRACTOT",f3d_fractot_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D RUP files into the "26_f3d_RUP" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D RUP FILE INTO TABLE 26_F3D_RUP")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['TIME','ELEM','SEGM','FIBER']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_rup_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/RUP",names=header_list,skiprows=2,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_rup_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_rup_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_rup_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_rup_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_rup_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","26_f3d_RUP",f3d_rup_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","26_f3d_RUP",f3d_rup_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D FAIL files into the "27_f3d_FAIL" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D FAIL FILE INTO TABLE 27_F3D_FAIL")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['TIME','ELEM','SEGM']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_fail_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/FAIL",names=header_list,skiprows=2,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_fail_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_fail_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_fail_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_fail_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_fail_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","27_f3d_FAIL",f3d_fail_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","27_f3d_FAIL",f3d_fail_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D XDRFT files into the "28_f3d_XDRFT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D XDRFT FILE INTO TABLE 28_F3D_XDRFT")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['NUMBER','XDRIFT']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_xdrft_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/XDRFT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_xdrft_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_xdrft_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_xdrft_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_xdrft_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_xdrft_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","28_f3d_XDRFT",f3d_xdrft_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","28_f3d_XDRFT",f3d_xdrft_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D YDRFT files into the "29_f3d_YDRFT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D XDRFT FILE INTO TABLE 28_F3D_XDRFT")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['NUMBER','YDRIFT']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_ydrft_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/YDRFT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_ydrft_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_ydrft_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_ydrft_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_ydrft_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_ydrft_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","29_f3d_YDRFT",f3d_ydrft_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","29_f3d_YDRFT",f3d_ydrft_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D AVGPKDRFT files into the "30_f3d_AVGPKDRFT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D AVGPKDRFT FILE INTO TABLE 30_F3D_AVGPKDRFT")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['AVGPKDRIFT']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_avgpkdrft_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/AVGPKDRFT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_avgpkdrft_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_avgpkdrft_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_avgpkdrft_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_avgpkdrft_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_avgpkdrft_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","30_f3d_AVGPKDRFT",f3d_avgpkdrft_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","30_f3d_AVGPKDRFT",f3d_avgpkdrft_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D XDRFTRTH files into the "31_f3d_XDRFTRTH" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D XDRFTRTH FILE INTO TABLE 31_F3D_XDRFTRTH")
    file_path_avd = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['R001', 'R002', 'R003', 'R004', 'R005', 'R006', 'R007', 'R008', 'R009', 'R010',
                        'R011', 'R012', 'R013', 'R014', 'R015', 'R016', 'R017', 'R018', 'R019', 'R020',
                        'R021', 'R022', 'R023', 'R024', 'R025', 'R026', 'R027', 'R028', 'R029', 'R030',
                        'R031', 'R032', 'R033', 'R034', 'R035', 'R036', 'R037', 'R038', 'R039', 'R040',
                        'R041', 'R042', 'R043', 'R044', 'R045', 'R046', 'R047', 'R048', 'R049', 'R050',
                        'R051', 'R052', 'R053', 'R054', 'R055', 'R056', 'R057', 'R058', 'R059', 'R060',
                        'R061', 'R062', 'R063', 'R064', 'R065', 'R066', 'R067', 'R068', 'R069', 'R070',
                        'R071', 'R072', 'R073', 'R074', 'R075', 'R076', 'R077', 'R078', 'R079', 'R080',
                        'R081', 'R082', 'R083', 'R084', 'R085', 'R086', 'R087', 'R088', 'R089', 'R090',
                        'R091', 'R092', 'R093', 'R094', 'R095', 'R096', 'R097', 'R098', 'R099', 'R100'
                        ]
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            gm_avdx_df = pd.read_csv(file_path_avd + "/" + item2 + ".HXE.avdx", names=header_list_avdx, skiprows=0,
                                     skipinitialspace=True, sep='\s+')
            f3d_xdrftrth_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/XDRFTRTH",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING','TIME'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_xdrftrth_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_xdrftrth_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_xdrftrth_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_xdrftrth_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], gm_avdx_df['TIME'], f3d_xdrftrth_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","31_f3d_XDRFTRTH",f3d_xdrftrth_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","31_f3d_XDRFTRTH",f3d_xdrftrth_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D XDRFTRTH files into the "32_f3d_YDRFTRTH" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D YDRFTRTH FILE INTO TABLE 32_F3D_YDRFTRTH")
    file_path_avd = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['R001', 'R002', 'R003', 'R004', 'R005', 'R006', 'R007', 'R008', 'R009', 'R010',
                        'R011', 'R012', 'R013', 'R014', 'R015', 'R016', 'R017', 'R018', 'R019', 'R020',
                        'R021', 'R022', 'R023', 'R024', 'R025', 'R026', 'R027', 'R028', 'R029', 'R030',
                        'R031', 'R032', 'R033', 'R034', 'R035', 'R036', 'R037', 'R038', 'R039', 'R040',
                        'R041', 'R042', 'R043', 'R044', 'R045', 'R046', 'R047', 'R048', 'R049', 'R050',
                        'R051', 'R052', 'R053', 'R054', 'R055', 'R056', 'R057', 'R058', 'R059', 'R060',
                        'R061', 'R062', 'R063', 'R064', 'R065', 'R066', 'R067', 'R068', 'R069', 'R070',
                        'R071', 'R072', 'R073', 'R074', 'R075', 'R076', 'R077', 'R078', 'R079', 'R080',
                        'R081', 'R082', 'R083', 'R084', 'R085', 'R086', 'R087', 'R088', 'R089', 'R090',
                        'R091', 'R092', 'R093', 'R094', 'R095', 'R096', 'R097', 'R098', 'R099', 'R100'
                        ]
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            gm_avdx_df = pd.read_csv(file_path_avd + "/" + item2 + ".HXE.avdx", names=header_list_avdx, skiprows=0,
                                     skipinitialspace=True, sep='\s+')
            f3d_ydrftrth_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/YDRFTRTH",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING','TIME'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_ydrftrth_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_ydrftrth_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_ydrftrth_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_ydrftrth_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], gm_avdx_df['TIME'], f3d_ydrftrth_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","32_f3d_YDRFTRTH",f3d_ydrftrth_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","32_f3d_YDRFTRTH",f3d_ydrftrth_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D PKDRFT files into the "33_f3d_PKDRFT" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D PKDRFT FILE INTO TABLE 33_F3D_PKDRFT")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['PKDRIFT']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_pkdrft_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/PKDRFT",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_pkdrft_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_pkdrft_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_pkdrft_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_pkdrft_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_pkdrft_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","33_f3d_PKDRFT",f3d_pkdrft_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","33_f3d_PKDRFT",f3d_pkdrft_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D PERF files into the "34_f3d_PERF" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D PERF FILE INTO TABLE 34_F3D_PERF")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['BLDG_PERF_LVL']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_perf_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/PERF",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_perf_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_perf_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_perf_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_perf_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_perf_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","34_f3d_PERF",f3d_perf_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","34_f3d_PERF",f3d_perf_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D ELMGRPRES files into the "35_f3d_ELMGRPRES" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D ELMGRPRES FILE INTO TABLE 35_F3D_ELMGRPRES")
    file_path_avd = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/spec/avddir"
    header_list_avdx = ['TIME','ACCX','VELX','DISX']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1/exist"
    f3d_elmgrpres_id_df = pd.read_csv(file_path+"/ELMGRPRES_ID",names=header_list,skiprows=1,skipinitialspace=True,sep='\s+')
    ngrp = len(f3d_elmgrpres_id_df)
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['GRP_ID','FX','FY','FZ','MX','MY','MZ']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            gm_avdx_df = pd.read_csv(file_path_avd + "/" + item2 + ".HXE.avdx", names=header_list_avdx, skiprows=0,
                                     skipinitialspace=True, sep='\s+')
            f3d_elmgrpres_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/ELMGRPRES",names=header_list,skiprows=ngrp,skipinitialspace=True,sep='\s+')
            time_full = pd.DataFrame(columns=['TIME'])
            time_short = pd.DataFrame(index=range(0,ngrp), columns=['TIME'])
            if ngrp!= 0:
                for i in range(len(gm_avdx_df)):
                    time_short['TIME'] = gm_avdx_df.iloc[i,0]
                    time_full = time_full.append(time_short,ignore_index=True)
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING','TIME'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_elmgrpres_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_elmgrpres_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_elmgrpres_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_elmgrpres_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], time_full['TIME'], f3d_elmgrpres_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","35_f3d_ELMGRPRES",f3d_elmgrpres_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","35_f3d_ELMGRPRES",f3d_elmgrpres_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D DAM_IND_EF files into the "36_f3d_DAM_IND_EF" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D DAM_IND_EF FILE INTO TABLE 36_F3D_DAM_IND_EF")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['IDFIBEL','ELEM','IDSEG','DAM_IND']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_dam_ind_ef_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/DAM_IND_EF",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_dam_ind_ef_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_dam_ind_ef_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","36_f3d_DAM_IND_EF",f3d_dam_ind_ef_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","36_f3d_DAM_IND_EF",f3d_dam_ind_ef_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D DAM_IND_EF5_END files into the "37_f3d_DAM_IND_EF5_END" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D DAM_IND_EF5_END FILE INTO TABLE 37_F3D_DAM_IND_EF5_END")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['IDFIBEL5','ELEM','IDSEG','DAM_IND']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_dam_ind_ef5_end_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/DAM_IND_EF5_END",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef5_end_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef5_end_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef5_end_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_dam_ind_ef5_end_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_dam_ind_ef5_end_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","37_f3d_DAM_IND_EF5_END",f3d_dam_ind_ef5_end_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","37_f3d_DAM_IND_EF5_END",f3d_dam_ind_ef5_end_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D DAM_IND_EF5_MID files into the "38_f3d_DAM_IND_EF5_MID" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D DAM_IND_EF5_MID FILE INTO TABLE 38_F3D_DAM_IND_EF5_MID")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['IDFIBEL5','ELEM','IDSEG','DAM_IND']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_dam_ind_ef5_mid_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/DAM_IND_EF5_MID",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef5_mid_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef5_mid_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_ef5_mid_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_dam_ind_ef5_mid_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_dam_ind_ef5_mid_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","38_f3d_DAM_IND_EF5_MID",f3d_dam_ind_ef5_mid_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","38_f3d_DAM_IND_EF5_MID",f3d_dam_ind_ef5_mid_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0

# Ingesting FRAME3D DAM_IND_PZ1 files into the "39_f3d_DAM_IND_PZ1" table of the rupture_to_rafters_db schema for all buildings in the bldg_dbfile
exec_flag = 0
while exec_flag==1:
    print("INGESTING FRAME3D DAM_IND_PZ1 FILE INTO TABLE 39_F3D_DAM_IND_PZ1")
    file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
    header_list = ['EARTHQUAKE','MAGNITUDE','SYNTH_OR_REAL','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4']
    eqke_df = pd.read_csv(file_path+"/eqke_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    header_list = ['STATION']
    stations_df = pd.read_csv(file_path+"/stations.csv",names=header_list,skiprows=1)
    header_list = ['BUILDING','UNITS','PUB_REF_1','PUB_REF_2','PUB_REF_3','PUB_REF_4','PUB_REF_5']
    bldg_df = pd.read_csv(file_path+"/bldg_dbfile.csv",names=header_list,skiprows=1,skipinitialspace=True,sep=',')
    bldgs = ['exist', 'optim4', 'optim5']
# Whitespace delimiter: sep='\s+'
    count = 0
    for item in bldg_df["BUILDING"]:
        header_list = ['IDPZ1','NODE','NPZZORY','DAM_IND']
        file_path = "../../vm-shared/optimf/1982UBC/SAFDEN/rup1"
        count2 = 0
        for item2 in stations_df["STATION"]:
            f3d_dam_ind_pz1_df = pd.read_csv(file_path+"/"+bldgs[count]+"/output"+"/"+item2+"/DAM_IND_PZ1",names=header_list,skiprows=0,skipinitialspace=True,sep='\s+')
            header_list_tab = ['EARTHQUAKE','STATION','BUILDING'] + header_list
# create a one column dataframe with the name of the bldg
            eqke_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_pz1_df)), columns=['EARTHQUAKE'])
            eqke_df_full['EARTHQUAKE'] = eqke_df.loc[0:1,'EARTHQUAKE'][0]
            bldg_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_pz1_df)),columns=['BUILDING'])
            bldg_df_full['BUILDING'] = bldg_df.loc[:,'BUILDING'][count]
            stations_df_full = pd.DataFrame(index=range(len(f3d_dam_ind_pz1_df)), columns=['STATION'])
            stations_df_full['STATION'] = item2
            f3d_dam_ind_pz1_tab_df = pd.concat([eqke_df_full['EARTHQUAKE'], stations_df_full['STATION'], bldg_df_full['BUILDING'], f3d_dam_ind_pz1_df], axis = 1)
            count2+=1
            add_data_to_table(mysql_engine,"rupture_to_rafters_db","39_f3d_DAM_IND_PZ1",f3d_dam_ind_pz1_tab_df,"append")
#            add_data_to_table(mysql_engine,"rupture_to_rafters_db","39_f3d_DAM_IND_PZ1",f3d_dam_ind_pz1_tab_df,"replace")
            print("Bldg: ",item,"  Station: ",item2,"  Count: ",count2)
        count+=1
    exec_flag = 0
