'''
Created on 9 Sep 2016

@author: wouter
'''

import getopt ,sys, os
import MySQLdb
from datetime import date
import datetime
from time import gmtime, strftime
import ftplib
import time 
import smtplib
import GenoDataDBTools

from email.mime.text import MIMEText

def send_mail(address, mailtext, subject):
    if address=="":
        return
    sender="sequencing.giga@ulg.ac.be"
    address_list=[address,]
    address_string=""
    for addressee in address_list:
        address_string=address_string+addressee+", "
    address_string=address_string[0:-2]
    msg=MIMEText(mailtext)
    msg["Subject"]= subject
    msg["From"]=sender
    msg["To"]=address_string
    s = smtplib.SMTP("smtp.ulg.ac.be")
    s.sendmail(sender, address_list, msg.as_string())
    s.quit()


def loadDPsubfile(serverpath,filename,IP_host,MarkerSet_ID):
    METHOD_NAME = "Load_DP_subfile"
    Process_ID = GenoDataDBTools.Process_start(METHOD_NAME)
    
    #db = MySQLdb.connect(db="GenoDataLabTmp", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    connTmp= MySQLdb.connect(db="GenoDataLabTmp", user="root", host=IP_host , passwd="CS1DB2TD3WC4")
    cursorTmp = connTmp.cursor(MySQLdb.cursors.DictCursor)
    
    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor()
 
    cursorTmp.execute ("Truncate tblghm_planned_tests_tmp")
    cursorTmp.execute ("LOAD DATA  INFILE '" + serverpath + filename + "' INTO TABLE tblghm_planned_tests_tmp  FIELDS TERMINATED BY ';' IGNORE 1 LINES;")
    print  "METHOD_NAME: " + METHOD_NAME + " records in tblghm_planned_tests_tmp " + str(cursorTmp.rowcount) 
    
    tblRegNumbersNew_tmp=GenoDataDBTools.CreateTmpTable('GenoDataLab_InnoDB','tblRegNumbersNew_tmp')
    
    #cursorInnoDB.execute ("Truncate tblRegNumbersNew_tmp")
    cursorInnoDB.execute ("INSERT INTO " + tblRegNumbersNew_tmp + "( Genotype, Retest,MarkerSet_ID, REG_NUMB_NP, REG_NUMB_NP_Type, Name, BirthDate, DateOfBirth, Gender, Farm_ID, Sire_ID_NP, Sire_ID_NP_Type, Dam_ID_NP, Dam_ID_NP_Type, status, SampleType, Project )  \
    SELECT if(tblghm_planned_tests_tmp.Genotype='Yes', if(tblghm_planned_tests_tmp.status='cancelled',0,-1),-1), if(tblghm_planned_tests_tmp.status='resent',-1,0),"+ str(MarkerSet_ID) +",tblghm_planned_tests_tmp.`Reg number`, 'Reg_Num' AS NP_Type, tblghm_planned_tests_tmp.Name, str_to_date(tblghm_planned_tests_tmp.`Date of Birth`,'%d-%m-%Y'), DATE_FORMAT(str_to_date(tblghm_planned_tests_tmp.`Date of Birth`,'%d-%m-%Y'),'%Y%m%d'), tblghm_planned_tests_tmp.Gender, \
    tblghm_planned_tests_tmp.UBN,COALESCE(NULLIF(trim(tblghm_planned_tests_tmp.Father), ''), '0') , 'Reg_Num' AS Sire_NP_Type, COALESCE(NULLIF(trim(tblghm_planned_tests_tmp.Mother), ''), '0'), 'Reg_Num' AS Dam_NP_Type, tblghm_planned_tests_tmp.Status, tblghm_planned_tests_tmp.Material, tblghm_planned_tests_tmp.Type FROM GenoDataLabTmp.tblghm_planned_tests_tmp as tblghm_planned_tests_tmp;")
    print  "METHOD_NAME: " + METHOD_NAME + " records in tblRegNumbersNew_tmp: " + str(cursorInnoDB.rowcount) 
    
    connTmp.commit()
    connInnoDB.commit()
    cursorTmp.close()    
    cursorInnoDB.close() 
    return tblRegNumbersNew_tmp

def loadHGsubfile(serverpath,filename,IP_host,MarkerSet_ID):
    METHOD_NAME = "Load_HG_subfile"
    Process_ID = GenoDataDBTools.Process_start(METHOD_NAME)
    
    #db = MySQLdb.connect(db="GenoDataLabTmp", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    connTmp= MySQLdb.connect(db="GenoDataLabTmp", user="root", host=IP_host , passwd="CS1DB2TD3WC4")
    cursorTmp = connTmp.cursor(MySQLdb.cursors.DictCursor)
    
    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor()
 
    cursorTmp.execute ("Truncate tblMER52_FOKIS_tmp")
    cursorTmp.execute ("LOAD DATA  INFILE '" + serverpath + filename + "' INTO TABLE tblMER52_FOKIS_tmp  FIELDS TERMINATED BY ',' IGNORE 1 LINES;")
    print  "METHOD_NAME: " + METHOD_NAME + " records in tblMER52_FOKIS_tmp " + str(cursorTmp.rowcount) 
    
    tblRegNumbersNew_tmp=GenoDataDBTools.CreateTmpTable('GenoDataLab_InnoDB','tblRegNumbersNew_tmp')
    
    
    
    cursorInnoDB.execute ("INSERT INTO " + tblRegNumbersNew_tmp + " (Genotype, Retest,MarkerSet_ID, REG_NUMB_NP, REG_NUMB_NP_Type, AltREG_NUMB_NP, AltREG_NUMB_NP_Type, Name, Short_Name, Straw_Name, DateOfBirth, Gender, AIcode, ABcode, Sire_ID_NP, Sire_ID_NP_Type, AltSire_ID_NP, AltSire_ID_NP_Type, Sire_Name, Dam_ID_NP, Dam_ID_NP_Type, AltDam_ID_NP, AltDam_ID_NP_Type, Dam_Name, Remark, CoatColor, Breed ) \
    SELECT if(tblMER52_FOKIS_tmp.Genotype='yes' or tblMER52_FOKIS_tmp.Genotype='ret',-1,0), if(tblMER52_FOKIS_tmp.Genotype='ret',-1,0) AS Retest ,"+ str(MarkerSet_ID) +", tblMER52_FOKIS_tmp.REG_NUMB_NP, tblMER52_FOKIS_tmp.REG_NUMB_NP_Type, tblMER52_FOKIS_tmp.AltREG_NUMB_NP, tblMER52_FOKIS_tmp.AltREG_NUMB_NP_Type, tblMER52_FOKIS_tmp.Name, tblMER52_FOKIS_tmp.ShortName, tblMER52_FOKIS_tmp.Straw_Name, tblMER52_FOKIS_tmp.DateOfBirth, tblMER52_FOKIS_tmp.Gender, tblMER52_FOKIS_tmp.AIcode,\
    tblMER52_FOKIS_tmp.ABcode, COALESCE(NULLIF(trim(tblMER52_FOKIS_tmp.Sire_ID_NP), ''), '0'), tblMER52_FOKIS_tmp.Sire_ID_NP_Type, COALESCE(NULLIF(trim(tblMER52_FOKIS_tmp.AltSire_ID_NP), ''), '0'), tblMER52_FOKIS_tmp.AltSire_ID_NP_Type, tblMER52_FOKIS_tmp.Sire_Name, COALESCE(NULLIF(trim(tblMER52_FOKIS_tmp.Dam_ID_NP), ''), '0'), tblMER52_FOKIS_tmp.Dam_ID_NP_Type, COALESCE(NULLIF(trim(tblMER52_FOKIS_tmp.AltDam_ID_NP), ''), '0'), tblMER52_FOKIS_tmp.AltDam_ID_NP_Type, tblMER52_FOKIS_tmp.Dam_Name, tblMER52_FOKIS_tmp.Remark, tblMER52_FOKIS_tmp.CoatColor, tblMER52_FOKIS_tmp.Breed \
    FROM GenoDataLabTmp.tblMER52_FOKIS_tmp as tblMER52_FOKIS_tmp;")
    print  "METHOD_NAME: " + METHOD_NAME + " records in tblRegNumbersNew_tmp: " + str(cursorInnoDB.rowcount)
    #COALESCE(NULLIF(trim(tblMER52_FOKIS_tmp.Dam_ID_NP), ''), '0')

    connTmp.commit()
    connInnoDB.commit()
    cursorTmp.close()    
    cursorInnoDB.close() 
    return tblRegNumbersNew_tmp

def Prep_tblRegNumbersNew_tmp(tblRegNumbersNew_tmp):
    METHOD_NAME = "Prep_tblRegNumbersNew_tmp"
    Process_ID = GenoDataDBTools.Process_start(METHOD_NAME)
    
    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor()
    
    
    cursorInnoDB.execute ("UPDATE ((((((tblRegConvOrgPrior INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegConvOrgPrior.Type = tblRegNumbersNew_tmp.REG_NUMB_NP_Type) LEFT JOIN \
    tblRegConvOrgPrior AS tblRegConvOrgPrior_1 ON tblRegNumbersNew_tmp.Sire_ID_NP_Type = tblRegConvOrgPrior_1.Type) LEFT JOIN tblRegConvOrgPrior AS tblRegConvOrgPrior_2 ON tblRegNumbersNew_tmp.Dam_ID_NP_Type = tblRegConvOrgPrior_2.Type) \
    LEFT JOIN tblRegConvOrgPrior AS tblRegConvOrgPrior_3 ON tblRegNumbersNew_tmp.AltSire_ID_NP_Type = tblRegConvOrgPrior_3.Type) LEFT JOIN tblRegConvOrgPrior AS tblRegConvOrgPrior_4 ON tblRegNumbersNew_tmp.AltDam_ID_NP_Type = tblRegConvOrgPrior_4.Type)\
     LEFT JOIN tblRegConvOrgPrior AS tblRegConvOrgPrior_5 ON tblRegNumbersNew_tmp.AltREG_NUMB_NP_Type = tblRegConvOrgPrior_5.Type) LEFT JOIN tblRegConvOrgPrior AS tblRegConvOrgPrior_6 ON tblRegNumbersNew_tmp.Alt2REG_NUMB_NP_Type = tblRegConvOrgPrior_6.Type \
    SET tblRegNumbersNew_tmp.REG_NUMB = if(Length(tblRegConvOrgPrior.Label)>0, concat(tblRegConvOrgPrior.Label,Trim(REG_NUMB_NP)),Trim(REG_NUMB_NP)),\
    tblRegNumbersNew_tmp.REG_NUMB_Type = if(Length(tblRegConvOrgPrior.Label)>0, tblRegConvOrgPrior.AfterPrefix,REG_NUMB_NP_Type), \
    tblRegNumbersNew_tmp.Sire_ID = if(Sire_ID_NP<>'0',if(Length(tblRegConvOrgPrior_1.Label)>0, concat(tblRegConvOrgPrior_1.Label,Trim(Sire_ID_NP)),Trim(Sire_ID_NP)),'0'), \
    tblRegNumbersNew_tmp.Sire_ID_Type = if(Sire_ID_NP<>'0',if(Length(tblRegConvOrgPrior_1.Label)>0,tblRegConvOrgPrior_1.AfterPrefix,Trim(Sire_ID_NP_Type)),''),\
    tblRegNumbersNew_tmp.AltSire_ID = if(AltSire_ID_NP<>'0',if(Length(tblRegConvOrgPrior_3.Label)>0, concat(tblRegConvOrgPrior_3.Label,Trim(AltSire_ID_NP)),Trim(AltSire_ID_NP)),'0'), \
    tblRegNumbersNew_tmp.AltSire_ID_Type = if(AltSire_ID_NP<>'0',if(Length(tblRegConvOrgPrior_3.Label)>0, tblRegConvOrgPrior_3.AfterPrefix,Trim(AltSire_ID_NP_Type)),''),\
    tblRegNumbersNew_tmp.AltDam_ID = if(AltDam_ID_NP<>'0',if(Length(tblRegConvOrgPrior_4.Label)>0, concat(tblRegConvOrgPrior_4.Label,Trim(AltDam_ID_NP)),Trim(AltDam_ID_NP)),'0'),\
    tblRegNumbersNew_tmp.AltDam_ID_Type = if(AltDam_ID_NP<>'0',if(Length(tblRegConvOrgPrior_4.Label)>0,tblRegConvOrgPrior_4.afterprefix,Trim(AltDam_ID_NP_Type)),''),\
    tblRegNumbersNew_tmp.Dam_ID = if(dam_ID_NP<>'0',if(Length(tblRegConvOrgPrior_2.Label)>0, concat(tblRegConvOrgPrior_2.Label,Trim(Dam_ID_NP)),Trim(Dam_ID_NP)),'0'),\
    tblRegNumbersNew_tmp.Dam_ID_Type = if(dam_ID_NP<>'0',if(Length(tblRegConvOrgPrior_2.Label)>0, tblRegConvOrgPrior_2.afterprefix,Trim(Dam_ID_NP_Type)),''),\
    tblRegNumbersNew_tmp.AltREG_NUMB = if(AltREG_NUMB_NP<>'0',if(Length(tblRegConvOrgPrior_5.Label)>0, concat(tblRegConvOrgPrior_5.Label,Trim(AltREG_NUMB_NP)),Trim(AltREG_NUMB_NP)),'0'),\
    tblRegNumbersNew_tmp.AltREG_NUMB_Type = if(AltReg_Numb_NP<>'0',if(Length(tblRegConvOrgPrior_5.Label)>0, tblRegConvOrgPrior_5.afterprefix,Trim(AltREG_NUMB_NP_Type)),''), \
    tblRegNumbersNew_tmp.Alt2REG_NUMB = if(Alt2REG_NUMB_NP<>'0',if(Length(tblRegConvOrgPrior_6.Label)>0, concat(tblRegConvOrgPrior_6.Label,Trim(Alt2REG_NUMB_NP)),Trim(Alt2REG_NUMB_NP)),'0'),\
    tblRegNumbersNew_tmp.Alt2REG_NUMB_Type = if(Alt2Reg_Numb_NP<>'0',if(Length(tblRegConvOrgPrior_6.Label)>0, tblRegConvOrgPrior_6.afterprefix,Trim(Alt2REG_NUMB_NP_Type)),'');")
    print  "METHOD_NAME: " + METHOD_NAME + " updated afterprefix in tblRegNumbersNew_tmp: " + str(cursorInnoDB.rowcount) 
    
    cursorInnoDB.execute ("UPDATE " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp LEFT JOIN tblRegNumbers ON tblRegNumbersNew_tmp.REG_NUMB = tblRegNumbers.REG_NUMB SET tblRegNumbersNew_tmp.Ref_REG_NUMB = If(IsNull(tblRegNumbers.REF_REG_NUMB) Or (tblRegNumbers.REF_REG_NUMB)='' Or (tblRegNumbers.REF_REG_NUMB)='0',tblRegNumbersNew_tmp.REG_NUMB,tblRegNumbers.REF_REG_NUMB), tblRegNumbersNew_tmp.Ref_REG_NUMB_Type = If(IsNull(tblRegNumbers.REF_REG_NUMB_Type) Or (tblRegNumbers.REF_REG_NUMB_Type)='' Or (tblRegNumbers.REF_REG_NUMB_Type)='0',tblRegNumbersNew_tmp.REG_NUMB_Type,tblRegNumbers.REF_REG_NUMB_Type);")
    
    print  "METHOD_NAME: " + METHOD_NAME + " updated RefRegnumbers in tblRegNumbersNew_tmp: " + str(cursorInnoDB.rowcount) 
    
    GenoDataDBTools.Process_end(Process_ID)
    connInnoDB.commit()  
    cursorInnoDB.close() 

def RunAddNewPed(UpdateDB, ForcePedUpdate , Remark, Origin,tblRegNumbersNew_tmp):

    METHOD_NAME = "AddRegNumersNew"
    UserLogin= 'Script'
    Process_ID = GenoDataDBTools.Process_start(METHOD_NAME)
    
    #Call Prep_RegNumbersNew
    
    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor()
 
    parent = ["Sire", "Dam"] #i+1= gender
    names = ["Name", "Short_Name", "Straw_Name"]
    
    cursorInnoDB.execute ("UPDATE  tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.Remark = concat(tblRegNumbers.Remark,';',Trim(tblRegNumbersNew_tmp.Remark));")
    #dam
    for i in (0,1):
    #i+1= gender
        cursorInnoDB.execute ("INSERT INTO tblRegNumbers ( REG_NUMB, REG_NUMB_Type, REG_NUMB_NP, REG_NUMB_NP_Type, Name, Name_CL, Sexe, Origine, Created, LastUpdate, SIRE_ID, DAM_ID, Remark, Species, UserLogin ) \
        SELECT DISTINCT tblRegNumbersNew_tmp." + parent[i] + "_ID, tblRegNumbersNew_tmp." + parent[i] + "_ID_Type, tblRegNumbersNew_tmp." + parent[i] + "_ID_NP, tblRegNumbersNew_tmp." + parent[i] + "_ID_NP_Type, tblRegNumbersNew_tmp." + parent[i] + "_Name, tblRegNumbersNew_tmp." + parent[i] + "_Name_CL, " + str(i+1) + " AS Gender, '" + Origin + "' AS Origine,  \
        Now() AS Created, Now() AS LastUpdate, '0' as SIRE_SIRE_ID , '0' as  DAM_DAM_ID, '" + Remark + "' AS Remark, tblRegNumbersNew_tmp.Species, '" + UserLogin + "' AS UserLogin \
        FROM " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp LEFT JOIN tblRegNumbers ON tblRegNumbersNew_tmp." + parent[i] + "_ID = tblRegNumbers.REG_NUMB WHERE (((tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'0' And (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'' And Not (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP) Is Null And (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'') AND ((tblRegNumbers.REG_NUMB) Is Null));")
        print  "METHOD_NAME: " + METHOD_NAME + " Added " + str(cursorInnoDB.rowcount)  + " " + parent[i] + " RegNumbers to tblRegNumbers"
        
    cursorInnoDB.execute ("INSERT INTO tblRegNumbers ( REF_REG_NUMB, REF_REG_NUMB_Type, REG_NUMB, REG_NUMB_NP, REG_NUMB_NP_Type, REG_NUMB_Type, Name, Name_CL, Short_Name, Sexe, Origine, Created, LastUpdate, SIRE_ID_NP, SIRE_ID_NP_Type, SIRE_ID, SIRE_ID_Type, DAM_ID, DAM_ID_Type, DAM_ID_NP, DAM_ID_NP_Type, AIcode, ABcode, BreedCode, Remark, DateOfBirth, Species, UserLogin ) \
    SELECT DISTINCT tblRegNumbersNew_tmp.Ref_REG_NUMB, tblRegNumbersNew_tmp.Ref_REG_NUMB_Type, tblRegNumbersNew_tmp.REG_NUMB, tblRegNumbersNew_tmp.REG_NUMB_NP, tblRegNumbersNew_tmp.REG_NUMB_NP_Type, tblRegNumbersNew_tmp.REG_NUMB_Type, tblRegNumbersNew_tmp.Name, tblRegNumbersNew_tmp.Name_CL, tblRegNumbersNew_tmp.Short_Name, tblRegNumbersNew_tmp.Gender,'" + Origin + "' AS Origin, \
    Now() AS Created, Now() AS LastUpdate, tblRegNumbersNew_tmp.Sire_ID_NP , tblRegNumbersNew_tmp.Sire_ID_NP_Type, tblRegNumbersNew_tmp.Sire_ID, if(IsNull(tblRegNumbersNew_tmp.Sire_ID_Type) Or tblRegNumbersNew_tmp.Sire_ID_Type='','UNK',tblRegNumbersNew_tmp.Sire_ID_Type) AS Sire_ID_Typ, tblRegNumbersNew_tmp.Dam_ID, if(IsNull(tblRegNumbersNew_tmp.Dam_ID_Type) Or tblRegNumbersNew_tmp.Dam_ID_Type='','UNK',tblRegNumbersNew_tmp.Dam_ID_Type) AS Dam_ID_Type, \
    tblRegNumbersNew_tmp.Dam_ID_NP, tblRegNumbersNew_tmp.Dam_ID_NP_Type, if(IsNull(tblRegNumbersNew_tmp.AIcode),0,tblRegNumbersNew_tmp.AIcode) AS Expr6, tblRegNumbersNew_tmp.ABcode, tblRegNumbersNew_tmp.Breed, concat(Trim(tblRegNumbersNew_tmp.Remark),' ','" + Remark + "', ';', AltREG_NUMB_NP_Type, ':' ,AltREG_NUMB_NP) AS Expr8, tblRegNumbersNew_tmp.DateOfBirth, tblRegNumbersNew_tmp.Species,'" + UserLogin + "' AS UserLogin \
    FROM tblRegNumbers RIGHT JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB WHERE (((tblRegNumbersNew_tmp.REG_NUMB_NP)<>'0' And Not (tblRegNumbersNew_tmp.REG_NUMB_NP) Is Null And (tblRegNumbersNew_tmp.REG_NUMB_NP)<>'') AND ((tblRegNumbers.REG_NUMB) Is Null));")
    print  "METHOD_NAME: " + METHOD_NAME + " Added " + str(cursorInnoDB.rowcount)  +  " RegNumbers to tblRegNumbers"


    if ForcePedUpdate==-1:
        for i in  (0,1):
            cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers." + parent[i] + "_ID = tblRegNumbersNew_tmp." + parent[i] + "_id, tblRegNumbers." + parent[i] + "_ID_Type = tblRegNumbersNew_tmp." + parent[i] + "_id_type, tblRegNumbers." + parent[i] + "_ID_NP = tblRegNumbersNew_tmp." + parent[i] + "_id_np, tblRegNumbers." + parent[i] + "_ID_NP_Type = tblRegNumbersNew_tmp." + parent[i] + "_id_np_type \
            WHERE (((tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'0' And (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'' And Not (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP) Is Null));")
            print  "METHOD_NAME: " + METHOD_NAME + " Force Updated " + str(cursorInnoDB.rowcount)  + " " + parent[i] + " in tblRegNumbers" 
        cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.Sexe = Gender, WHERE (((tblRegNumbersNew_tmp.Gender)<>3));")
        print  "METHOD_NAME: " + METHOD_NAME + " Force Updated " + str(cursorInnoDB.rowcount)  +  " genders in tblRegNumbers"
        cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.DateOfBirth = tblRegNumbersNew_tmp.DateOfBirth WHERE (((tblRegNumbers.DateOfBirth)<>tblRegNumbersNew_tmp.DateOfBirth) AND (Not (tblRegNumbersNew_tmp.DateOfBirth) Is Null));")
        print  "METHOD_NAME: " + METHOD_NAME + " Force Updated " + str(cursorInnoDB.rowcount)  +  " birth dates in tblRegNumbers"
    else:
        for i in  (0,1):
            cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers." + parent[i] + "_ID = tblRegNumbersNew_tmp." + parent[i] + "_id, tblRegNumbers." + parent[i] + "_ID_Type = tblRegNumbersNew_tmp." + parent[i] + "_id_type, tblRegNumbers." + parent[i] + "_ID_NP = tblRegNumbersNew_tmp." + parent[i] + "_id_np, tblRegNumbers." + parent[i] + "_ID_NP_Type = tblRegNumbersNew_tmp." + parent[i] + "_id_np_type WHERE (((tblRegNumbers." + parent[i] + "_ID_NP)='0' Or (tblRegNumbers." + parent[i] + "_ID_NP) Is Null Or (tblRegNumbers." + parent[i] + "_ID_NP)='') AND ((tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'0' And (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP)<>'' And Not (tblRegNumbersNew_tmp." + parent[i] + "_ID_NP) Is Null));")
            print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  + " " + parent[i] + " in tblRegNumbers" 
        cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.Sexe = Gender WHERE (((tblRegNumbers.Sexe)=3) AND ((tblRegNumbersNew_tmp.Gender)<>3));")
        print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " genders in tblRegNumbers"
        cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.DateOfBirth = tblRegNumbersNew_tmp.DateOfBirth WHERE (((tblRegNumbers.DateOfBirth) Is Null) AND (Not (tblRegNumbersNew_tmp.DateOfBirth) Is Null));")
        print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " birth dates in tblRegNumbers"

    for i in  (0,2):
        cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers." + names[i] + " = tblRegNumbersNew_tmp." + names[i] + " \
        WHERE (((tblRegNumbers." + names[i] + ") Is Null Or (tblRegNumbers." + names[i] + ")='') AND (Not (tblRegNumbersNew_tmp." + names[i] + ") Is Null And (tblRegNumbersNew_tmp." + names[i] + ")<>''));")
        print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " "  + names[i] + " in tblRegNumbers"

    cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.AIcode = tblRegNumbersNew_tmp.aicode WHERE (((tblRegNumbers.AIcode) Is Null Or (tblRegNumbers.AIcode)=0) AND ((tblRegNumbersNew_tmp.AIcode)<>0 And Not (tblRegNumbersNew_tmp.AIcode) Is Null));")
    print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " AIcode in tblRegNumbers"

    cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.ABcode = Cast(tblRegNumbersNew_tmp.abcode as signed integer) WHERE (((tblRegNumbers.ABcode) Is Null Or (tblRegNumbers.ABcode)='0' Or (tblRegNumbers.ABcode)='') AND ((tblRegNumbersNew_tmp.ABcode)<>'0' And Not (tblRegNumbersNew_tmp.ABcode) Is Null And (tblRegNumbersNew_tmp.ABcode)<>''));")
    print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " ABcode in tblRegNumbers"

    cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.Color = tblRegNumbersNew_tmp.CoatColor WHERE ((Not (tblRegNumbersNew_tmp.CoatColor) Is Null And (tblRegNumbersNew_tmp.CoatColor)<>''));")
    print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " Color in tblRegNumbers"

    cursorInnoDB.execute ("UPDATE tblRegNumbers INNER JOIN " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp ON tblRegNumbers.REG_NUMB = tblRegNumbersNew_tmp.REG_NUMB SET tblRegNumbers.BreedCode = tblRegNumbersNew_tmp.Breed WHERE ((Not (tblRegNumbersNew_tmp.Breed) Is Null And (tblRegNumbersNew_tmp.Breed)<>''));")
    print  "METHOD_NAME: " + METHOD_NAME + " Updated " + str(cursorInnoDB.rowcount)  +  " BreedCode in tblRegNumbers"
    
    for i in  (0,1):
        AltIDs = ["Alt" + parent[i] + "_ID_NP", parent[i] + "_ID_NP", "Alt" + parent[i] + "_ID_NP", parent[i] + "_ID_NP"]
        for y in  (0,3):
            for z in  (0,3):
                cursorInnoDB.execute ("INSERT IGNORE INTO tblRegConv ( RegNum, Type, RegNum_Alt, Type_Alt, Comment) \
                SELECT DISTINCT tblRegNumbersNew_tmp." + AltIDs[y] +  ", tblRegNumbersNew_tmp."  + AltIDs[y] +  "_Type, tblRegNumbersNew_tmp."  + AltIDs[z] + ", tblRegNumbersNew_tmp."  + AltIDs[z] + "_Type, '" + Remark + "' AS Comment \
                FROM " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp  WHERE ((Not (tblRegNumbersNew_tmp." + AltIDs[y] +  ") Is Null And (tblRegNumbersNew_tmp." + AltIDs[y] +  ")<>'0') AND (Not (tblRegNumbersNew_tmp."  + AltIDs[y] + "_Type) Is Null) and \
                (Not (tblRegNumbersNew_tmp."  + AltIDs[z] + ") Is Null And (tblRegNumbersNew_tmp." + AltIDs[z] + ")<>'0') AND (Not (tblRegNumbersNew_tmp."  + AltIDs[z] + "_Type) Is Null) );")
                print  "METHOD_NAME: " + METHOD_NAME + " Added " + str(cursorInnoDB.rowcount)  + " " + AltIDs[y] + " to " + AltIDs[z] + " to tblRegConv"
    
    AltIDs = ["REG_NUMB_NP", "REG_NUMB", "AltREG_NUMB_NP", "AltREG_NUMB", "Alt2REG_NUMB_NP", "Alt2REG_NUMB"]
    for i in  (0,5):
        for z in  (0,5):
            cursorInnoDB.execute ("INSERT IGNORE INTO tblRegConv ( RegNum, Type, RegNum_Alt, Type_Alt, Comment) \
            SELECT DISTINCT tblRegNumbersNew_tmp." + AltIDs[y] + ", tblRegNumbersNew_tmp." + AltIDs[y] + "_Type, tblRegNumbersNew_tmp." + AltIDs[z] + ", tblRegNumbersNew_tmp." + AltIDs[z] + "_Type, '" + Remark + "' AS Comment \
            FROM " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp WHERE ((Not (tblRegNumbersNew_tmp." + AltIDs[y] + ") Is Null And (tblRegNumbersNew_tmp." + AltIDs[y] + ")<>'0') AND (Not (tblRegNumbersNew_tmp." + AltIDs[y] + "_Type) Is Null) and  \
            (Not (tblRegNumbersNew_tmp." + AltIDs[z] + ") Is Null And (tblRegNumbersNew_tmp." + AltIDs[z] + ")<>'0') AND (Not (tblRegNumbersNew_tmp." + AltIDs[z] + "_Type) Is Null));")                    
            print  "METHOD_NAME: " + METHOD_NAME + " Added " + str(cursorInnoDB.rowcount)  + " " + AltIDs[y] + " to " + AltIDs[z] + " to tblRegConv"
    
     
    AltIDs = ["replace(INSERT(REG_NUMB, LOCATE(' ', REG_NUMB), CHAR_LENGTH(' '), '_'), ' ', '')", "replace(INSERT(REG_NUMB_NP, LOCATE(' ', REG_NUMB_NP), CHAR_LENGTH(' '), '_'), ' ', '')", "REG_NUMB_NP", "REG_NUMB"]
    AltIDs_Types = ["'EMBEMB'", "'EMB'", "REG_NUMB_NP_Type", "REG_NUMB_Type"]
    for y in  (0,3):
        for z in  (0,3):
            cursorInnoDB.execute ("INSERT INTO tblRegConv ( RegNum, Type, RegNum_Alt, Type_Alt)  SELECT  " + AltIDs[y] + "," + AltIDs_Types[y] + "," + AltIDs[z] + "," + AltIDs_Types[z] + " from " \
            + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp WHERE (((tblRegNumbersNew_tmp.REG_NUMB_Type) Like '%EMBHG'));")
            print  "METHOD_NAME: " + METHOD_NAME + " Added " + str(cursorInnoDB.rowcount)  + " " + AltIDs[y] + "," + AltIDs_Types[y] + " to " + AltIDs[z] + "," + AltIDs_Types[y] + " to tblRegConv" 
    
    if UpdateDB==-1:
        '''
        Call RegConvert
        Call UploadPed
        Call RefRegUpdate
        Call Update_RefRegNumNew
        'update refregnumber in new ped table. If not present yet update to reg_number
        '''
    
    connInnoDB.commit()
    cursorInnoDB.close()    
    GenoDataDBTools.Process_end(Process_ID)
    print "End RunAddNewPed"

def SetRegNumersNewToPrep(addordelete,Project,Exclude_genotyped,tblRegNumbersNew_tmp):
    METHOD_NAME = "SetRegNumersNewToPrep"
    Process_ID = GenoDataDBTools.Process_start(METHOD_NAME)

    NotSetToPrepare_MinCallRate = GenoDataDBTools.GetParameterValue("NotSetToPrepare_MinCallRate", "SetRegNumersNewToPrep")

    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor()

    if addordelete <> 3: #3 means excluding the animals in the list
        if addordelete == 2: #replace: reset all
            cursorInnoDB.execute ("UPDATE tblRegNumbers SET tblRegNumbers.Selected = 0 WHERE (((tblRegNumbers.Selected)<>0));")
            print  "METHOD_NAME: " + METHOD_NAME + "  Reset tblRegNumbers.Selected to 0: " + str(cursorInnoDB.rowcount) 
        elif addordelete == 4 : #reset project, reset only animals belonning to the project
            cursorInnoDB.execute ("UPDATE tblPriorities INNER JOIN (tblRegNumbers INNER JOIN tblOrderedTests ON tblRegNumbers.REG_NUMB = tblOrderedTests.REGNR) ON tblPriorities.Priority = tblOrderedTests.PRIORITY_Old SET tblRegNumbers.Selected = 0 \
            WHERE (((tblPriorities.Project)='" + Project + "'));")
            print  "METHOD_NAME: " + METHOD_NAME + "  Reset tblRegNumbers.Selected to 0 for the project " + Project + " " + str(cursorInnoDB.rowcount) 

    
        if Exclude_genotyped==1: 
            print  "Set Selected Animals if not genotyped yet;  Retest samples will be set to be genotyped anyway."
            cursorInnoDB.execute ("UPDATE " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp INNER JOIN tblRegNumbers ON (tblRegNumbersNew_tmp.Ref_REG_NUMB = tblRegNumbers.REF_REG_NUMB) AND (tblRegNumbersNew_tmp.Ref_REG_NUMB_Type = tblRegNumbers.REF_REG_NUMB_Type) SET tblRegNumbers.Selected =-1  WHERE (((tblRegNumbersNew_tmp.Genotype)<> 0));")
            print  "METHOD_NAME: " + METHOD_NAME + "  Set tblRegNumbers.Selected to -1 " + str(cursorInnoDB.rowcount) 
            cursorInnoDB.execute ("UPDATE GenoData.tblTestsIllumRaw as tblTestsIllumRaw INNER JOIN (" + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp INNER JOIN tblRegNumbers ON (tblRegNumbersNew_tmp.Ref_REG_NUMB = tblRegNumbers.REF_REG_NUMB) AND (tblRegNumbersNew_tmp.Ref_REG_NUMB_Type = tblRegNumbers.REF_REG_NUMB_Type)) ON (tblTestsIllumRaw.REF_REG_NUMBER = tblRegNumbersNew_tmp.Ref_REG_NUMB) AND (tblTestsIllumRaw.REF_TYPE = tblRegNumbersNew_tmp.Ref_REG_NUMB_Type) SET tblRegNumbers.Selected = 0 WHERE (((tblRegNumbers.Selected)<>0) AND ((tblRegNumbersNew_tmp.Genotype)<>0) AND ((tblTestsIllumRaw.CallRate)>=" + str(NotSetToPrepare_MinCallRate) + ") AND ((tblRegNumbersNew_tmp.Retest)<>-1));")
            print  "METHOD_NAME: " + METHOD_NAME + "  Set tblRegNumbers.Selected to 0 for already  genotyped at call rate: " +str(NotSetToPrepare_MinCallRate) +" " + str(cursorInnoDB.rowcount) 
        else:    
            cursorInnoDB.execute ("UPDATE " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp INNER JOIN tblRegNumbers ON (tblRegNumbersNew_tmp.Ref_REG_NUMB = tblRegNumbers.REF_REG_NUMB) AND (tblRegNumbersNew_tmp.Ref_REG_NUMB_Type = tblRegNumbers.REF_REG_NUMB_Type) SET tblRegNumbers.Selected =-1 WHERE (((tblRegNumbersNew_tmp.Genotype)<>0));")
            print  "METHOD_NAME: " + METHOD_NAME + "  Set tblRegNumbers.Selected to -1 " + str(cursorInnoDB.rowcount) 
    elif addordelete == 3:
        cursorInnoDB.execute ("UPDATE " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp INNER JOIN tblRegNumbers ON (tblRegNumbersNew_tmp.Ref_REG_NUMB = tblRegNumbers.REF_REG_NUMB) AND (tblRegNumbersNew_tmp.Ref_REG_NUMB_Type = tblRegNumbers.REF_REG_NUMB_Type) SET tblRegNumbers.Selected = 0;")
        print  "METHOD_NAME: " + METHOD_NAME + "  Set tblRegNumbers.Selected to 0 " + str(cursorInnoDB.rowcount) 
    
    print "End set RegNumbers to prep" 

    GenoDataDBTools.Process_end(Process_ID)
    connInnoDB.commit()
    cursorInnoDB.close() 

def  AddNewRegNumberToOrderedTests(prio,retestdate,filename,project, tblRegNumbersNew_tmp):
#Add animals to priolist
    METHOD_NAME = "AddNewRegNumberToOrderedTests"
    UserLogin= 'Script'
    Process_ID = GenoDataDBTools.Process_start(METHOD_NAME)
    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor(MySQLdb.cursors.DictCursor)
    
    countertypes=['Genotype','Retest','Status','Project']
    for i in  (0,3):
        cursorInnoDB.execute ("SELECT Count(tblRegNumbersNew_tmp.MarkerSet_ID) AS Count, tblRegNumbersNew_tmp." + countertypes[i] + " as " + countertypes[i] + " FROM " \
        + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp  GROUP BY tblRegNumbersNew_tmp." + countertypes[i] + ";")
        for row in cursorInnoDB:
            print  "METHOD_NAME: " + METHOD_NAME + " count " + countertypes[i] + "= " + str(row[countertypes[i]]) + ": "+ str(row['Count'])

    '''
    Create Priority: Priority can be mixed project
    the status reports are generated based on the project in the Priority table
    '''
    filename=file.split('/')[-1]
    
    cursorInnoDB.execute ("INSERT INTO tblPriorities ( Priority, Source, Submit_Date, Project, Active,UserLogin ) SELECT "  + str(prio) +  ", '" + filename + "'," + str(datetime.date.today().isoformat().replace("-", "")) + ", '" + project + "', -1 , '" + UserLogin + "' AS UserLogin;")

    cursorInnoDB.execute ("INSERT INTO tblOrderedTests ( MarkerSet_ID, PRIORITY, PRIORITY_Old, REGNR, Type, Ref_RegNum, Ref_Type, Retest, intReTestDate, SampleType, MarkerList, ConditionalFirstMarker, PrivateData, Owner, Farm_ID,Project, UserLogin ) \
    SELECT tblRegNumbersNew_tmp.MarkerSet_ID, " + str(prio) + ", " + str(prio) + ", tblRegNumbersNew_tmp.REG_NUMB, tblRegNumbersNew_tmp.REG_NUMB_Type, tblRegNumbersNew_tmp.Ref_REG_NUMB, tblRegNumbersNew_tmp.Ref_REG_NUMB_Type,\
    tblRegNumbersNew_tmp.Retest, " + str(retestdate) + ", tblRegNumbersNew_tmp.SampleType, tblRegNumbersNew_tmp.MarkerList, tblRegNumbersNew_tmp.ConditionalFirstMarker, tblRegNumbersNew_tmp.PrivateData, tblRegNumbersNew_tmp.Owner, tblRegNumbersNew_tmp.Farm_ID,tblRegNumbersNew_tmp.Project, '" + UserLogin + "' AS UserLogin \
    FROM " + tblRegNumbersNew_tmp +" as tblRegNumbersNew_tmp WHERE (((tblRegNumbersNew_tmp.Genotype)=-1));")
    print  "METHOD_NAME: " + METHOD_NAME + "  Added Animals to Prio "  + str(prio) +  ", project: " + project  + ": " + str(cursorInnoDB.rowcount) 
    
    GenoDataDBTools.Process_end(Process_ID)
    connInnoDB.commit()
    cursorInnoDB.close() 

    

def Usage ():
        sys.exit(0)
        
if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:t:", ["help", "input=","type="])
    except getopt.GetoptError, err:
            # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        Usage()
       
    for o, a in opts:
        if o in ("-h", "--help"):
            Usage()
        elif o in ("-i", "--input"):
            file = a
        elif o in ("-t", "--type"):
            Project = a    
        else:
            assert False, "unhandled option"   
                               
    try:
        if Project=='DP':   
            retestdate=int(file.split('.')[-2].split('_')[-1])
        elif Project=='HG':
            retestdate=file.split('~')[-1].split('-')[0]+file.split('~')[-1].split('-')[1]+file.split('~')[-1].split('-')[2]
        else:
            print 'type not correct: ' + Project
            Usage()
    except:
        print 'filename not correct: ' + file
        mail_text="Import Submission file " + file + " failed: filename not correct"
        subject= "Import Submission file " + file + " failed: filename not correct"
        send_mail("wouter.coppieters@ulg.ac.be", mail_text, subject)   
        Usage()

    
        
    serverpath="/"
        
    #filename="ghm_planned_tests_20160908.csv"
    host="localhost"
    MarkerSet_ID=15
    if Project=='DP':  
        tblRegNumbersNew_tmp=loadDPsubfile(serverpath,file,host,MarkerSet_ID)
    elif Project=='HG':
        tblRegNumbersNew_tmp=loadHGsubfile(serverpath,file,host,MarkerSet_ID)
    else:
        print 'type not correct: ' + Project
        Usage()
    
    Prep_tblRegNumbersNew_tmp(tblRegNumbersNew_tmp)
    
    UpdateDB=0
    ForcePedUpdate=0  
    Remark="autoplilot"
    Origin="HG"
    #Project="DP"
    
    connInnoDB = MySQLdb.connect(db="GenoDataLab_InnoDB", user="root", host="localhost", passwd="CS1DB2TD3WC4")
    cursorInnoDB= connInnoDB.cursor(MySQLdb.cursors.DictCursor)

    #cursorInnoDB.execute ("LOCK TABLES tblRegNumbersNew_tmp WRITE;")
    
    RunAddNewPed(UpdateDB, ForcePedUpdate , Remark, Origin,tblRegNumbersNew_tmp)
    
    addordelete=1
    Exclude_genotyped=0
    SetRegNumersNewToPrep(addordelete,Project,Exclude_genotyped,tblRegNumbersNew_tmp)
    
    cursorInnoDB.execute ("SELECT Max(tblPriorities.Priority) AS MaxOfPriority FROM tblPriorities WHERE (((tblPriorities.Project)='" + Project + "'));")
    row = cursorInnoDB.fetchone()
    prio =row['MaxOfPriority']+1
    print 'First Prio: '  + str(prio)


    AddNewRegNumberToOrderedTests(prio,retestdate,file,Project,tblRegNumbersNew_tmp)
    
    cursorInnoDB.execute ("DROP Table " + tblRegNumbersNew_tmp + ";")
    
    connInnoDB.commit()
    cursorInnoDB.close() 
    
    #cursorInnoDB.execute ("UNLOCK TABLES")