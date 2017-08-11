# encoding: utf-8
import requests
import MySQLdb as mdb
import time
import datetime
import logging
import json
import re
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.views.generic import CreateView, DeleteView, ListView, View
from .models import Picture
from .response import JSONResponse, response_mimetype
from .serialize import serialize
from subprocess import Popen, PIPE

logger = logging.getLogger(__name__)
database = settings.DATABASES['default']
regex = r'(^[^;]+)(.*)(?:\);)(.*$)?'

class PictureCreateView(CreateView):
    model = Picture
    fields = "__all__"

    def form_valid(self, form):
        self.object = form.save()
        files = [serialize(self.object)]
        data = {'files': files}
        response = JSONResponse(data, mimetype=response_mimetype(self.request))
        response['Content-Disposition'] = 'inline; filename=files.json'
        return response

    def form_invalid(self, form):
        data = json.dumps(form.errors)
        return HttpResponse(content=data, status=400, content_type='application/json')


class PictureDeleteView(DeleteView):
    model = Picture

    def delete(self, request, *args, **kwargs):
        self.object = self.get_object()
        self.object.delete()
        response = JSONResponse(True, mimetype=response_mimetype(request))
        response['Content-Disposition'] = 'inline; filename=files.json'
        return response


class PictureListView(ListView):
    model = Picture

    def render_to_response(self, context, **response_kwargs):
        files = [ serialize(p) for p in self.get_queryset() ]
        data = {'files': files}
        response = JSONResponse(data, mimetype=response_mimetype(self.request))
        response['Content-Disposition'] = 'inline; filename=files.json'
        return response


class ProcessCsvView(View):

    def __init__(self):
        self.sqlizer_url = 'https://sqlizer.io/api/files/'
        self.sqlizer_headers = {'Authorization':
                'Bearer lZicTcPoMhNGbzFmd_S_xox0G2RZ5SST026wUnKstQ88TTvZWa7eFbm3j1QUkelOm-4plUPZWGkhZL7I3ZVzkQ=='}
        self.status = ['Uploaded',
                'Analysing',
                'Processing',
                'Failed']

    def _monitor(self, file_id, response = None):
        r = requests.get(self.sqlizer_url + file_id + '/',
                headers = self.sqlizer_headers)
        if r.status_code is 200:
            if json.loads(r.text)['Status'] not in self.status and response != None:
                return json.loads(r.text)
            else:
                # TODO randomize time for re-check
                return self._monitor(file_id = file_id,
                        response = json.loads(r.text)['Status'])
        else:
            return {'error':
                    'Error monitoring file in sqlizer'}

    def get(self, request, *args, **kwargs):
        resource_id = request.path.split('/')[2]
        csv = Picture.objects.get(pk = resource_id)
        ts = csv.timestamp.strftime("%Y%m%d%H%M%S")
        tableName = 'nw' + csv.file_type + ts
        data = {'DatabaseType': 'MySQL',
            'FileType': 'csv',
            'FileName': csv.slug,
            'TableName': tableName,
            'FileHasHeaders': True
            }
        # sqlizer step 1
        r = requests.post(self.sqlizer_url, headers = self.sqlizer_headers,
                data = data)
        if r.status_code is 200:
            response = json.loads(r.text)
            file_id = response['ID']
            csv_file = {'file': csv.file}
            # sqlizer step 2
            u = requests.post(self.sqlizer_url + file_id + '/data/',
                    headers = self.sqlizer_headers,
                    files = csv_file)
            if u.status_code is 200:
                # sqlizer step 3
                p = requests.put(self.sqlizer_url + file_id + '/',
                        headers = self.sqlizer_headers,
                        data = {'Status': 'Uploaded'})
                if p.status_code is 200:
                    # sqlizer step 4, check until 'Complete' is returned
                    # TODO maybe _monitor should return status and percentage completed
                    # and keep checking for Completed asynchronously
                    response = self._monitor(file_id)
                    if response['Status'] == 'Complete':
                        sqlFile = requests.get(response['ResultUrl'])
                        if sqlFile.status_code is 200:
                            queries = sqlFile.content
                            #Execute queries
                            #Add ); because there are fields with ; inside
                            result = re.findall(regex, queries, re.MULTILINE)
                            #sqlCommands = queries.split(';')
                            conn = mdb.connect(host=database['HOST'],
                                    user=database['USER'],
                                    passwd=database['PASSWORD'],
                                    db=database['NAME'])
                            with conn:
                                cur = conn.cursor()
                                for command in result:
                                    sql=""
                                    for query in command:
                                        if query.strip() != "":
                                            sql=sql+query
                                    try:
                                        sql = sql.replace("\\","")
                                        cur.execute(sql+");")
                                    except Exception as e:
                                        response = {'error':e}

                            #Save file to disk
                            fileName = tableName+".sql"
                            with open(fileName, "w") as text_file:
                                text_file.write(queries)
                            text_file.close()
                        else:
                            response = {'error':
                                    'Error saving processed SQL file'}
                    else:
                        response = {'error': 'Error processing SQL file'}
                else:
                    response = {'error':
                            'Error trying to finalize file upload to sqlizer'}
            else:
                response = {'error':
                        'Error trying to upload file to sqlizer'}
        else:
            response = {'error':
                    'Error trying to initate file conversion on sqlizer'}
        #return HttpResponse(response)
        return JsonResponse(response, safe=False)


class PostProcessView(View):

    def __init__(self):
        pass

    def get(self, request, *args, **kwargs):

        salida={}
        files = request.GET.copy()
        TABLECAS=""
        TABLEPAT=""
        TABLEPATCAS=""
        TS = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')
        DATEFORMATTED=datetime.datetime.fromtimestamp(time.time()).strftime('%B %d, %Y at %H:%M:%S')

        sqlReport = ("INSERT INTO `explorer_query` "
            "VALUES (@MAXID,CONCAT('Patient_Record_Upload - ',"
            "'"+DATEFORMATTED+"'), "
            "'SELECT\r\n  pc.case_number,\r\n /* Req */ p.First_Name, \r\n    "
            "p.Middle_Name, \r\n /* Req */p.Last_Name, p.Date_of_Birth as DOB,\r\n   "
            "/* Req */CASE p.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" "
            "ELSE \"U\" END AS Gender,\r\n   /* Req */p.Social_Security_Number as SSN, "
            "p.Street_1 as Address1, \r\n  p.Street_2 as Address2,\r\n /* Req */p.City, "
            "p.State, p.Zip_Code as ZIP, \r\n   c.Marital_Status, p.Employment_Status, "
            "p.Chart_Number as Chart_No, p.Signature_on_File, \r\n    "
            "/* Req */coalesce(p.Phone_1, p.Contact_Phone_1) as Home_Phone, \r\n "
            "p.Work_Phone, \"\" as Cell_Phone, p.EMail, p.Race, p.Ethnicity,\r\n \r\n    \r\n    "
            "/* INSURANCE NUMBER 1 DATA  */\r\n  c.Insurance_Carrier_1 as HF_Payer_ID_P"
            ", \r\n    \"\" as Insurante_Type_P, \r\n  c.Group_Name_1 as Insurance_Group_Name_P,"
            "c.Group_Number_1 as Insurance_Group_Number_P, \r\n c.Policy_Number_1 as Insurance_ID_Number_P, "
            "c.Copayment_Amount as CoPay_P, \r\n c.Accept_Assignment_1 as Insured_Accept_Assingment_P, "
            "c.Insured_Relationship_1 as Patient_Relationship_P, \r\n  p1.First_Name as Insured_First_Name_P, "
            "p1.Middle_Initial as Insured_Middle_Initial_P, \r\n  p1.Last_Name as Insured_Last_Name_P, "
            "p1.Date_of_Birth as Insured_DOB_P, \r\n    CASE p1.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" "
            "then \"M\" ELSE \"U\" END AS Insured_Gender_P,\r\n p1.Street_1 as Insured_Address1_P, "
            "p1.Street_2 as Insured_Address2_P, p1.City as Insured_City_P, \r\n   p1.State as Insured_State_P, "
            "p1.Zip_Code as Insured_Zip_P, coalesce(p1.Phone_1, p1.Contact_Phone_1) as Insured_Phone_P,\r\n "
            "c.Prior_Authorization_No as Authorization_No_P, \r\n    \r\n    /* EMERGENCY CONTACT DATA */\r\n    "
            "left(p.Contact_Name, length(p.Contact_Name) - locate(\" \", reverse(p.Contact_Name))) as Emergency_First_Name, "
            "\r\n SUBSTRING_INDEX(p.Contact_Name, \" \", -1) as Emergency_Last_Name, \r\n COALESCE(p.Contact_Phone_1, "
            "p.Contact_Phone_2) as Emergency_Phone, \"Other\" as Emergency_Relation,\r\n  \r\n    "
            "/* INSURANCE NUMBER 2 DATA  */\r\n  c.Insurance_Carrier_2 as HF_Payer_ID_S, \r\n    \"\" as Insurante_Type_S, "
            "\r\n  c.Group_Name_2 as Insurance_Group_Name_S,c.Group_Number_2 as Insurance_Group_Number_S, \r\n "
            "c.Policy_Number_2 as Insurance_ID_Number_S,  \r\n   \"\" as CoPay_S,  \r\n  "
            "c.Accept_Assignment_2 as Insured_Accept_Assingment_S, c.Insured_Relationship_2 as Patient_Relationship_S, "
            "\r\n  p2.First_Name as Insured_First_Name_S, p2.Middle_Initial as Insured_Middle_Initial_S, \r\n  "
            "p2.Last_Name as Insured_Last_Name_S, p2.Date_of_Birth as Insured_DOB_S, \r\n    "
            "CASE p2.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Insured_Gender_S,"
            "\r\n p2.Street_1 as Insured_Address1_S, p2.Street_2 as Insured_Address2_S, p2.City as Insured_City_S, "
            "\r\n   p2.State as Insured_State_S, p2.Zip_Code as Insured_Zip_S, coalesce(p2.Phone_1, "
            "p2.Contact_Phone_1) as Insured_Phone_S,\r\n /*Falta*/ \"\"  as Authorization_No_S,\r\n  \r\n    "
            "/* INSURANCE NUMBER 3 DATA  */\r\n  c.Insurance_Carrier_3 as HF_Payer_ID_T, \r\n    "
            "/*Falta*/ \"\" as Insurante_Type_T, \r\n    c.Group_Name_3 as Insurance_Group_Name_T, "
            "c.Group_Number_3 as Insurance_Group_Number_T, \r\n    c.Policy_Number_3 as Insurance_ID_Number_T, \r\n    "
            "/*Falta*/ \"\" as CoPay_T, \r\n c.Accept_Assignment_3 as Insured_Accept_Assingment_T, "
            "c.Insured_Relationship_3 as Patient_Relationship_T, \r\n  p3.First_Name as Insured_First_Name_T, "
            "p3.Middle_Initial as Insured_Middle_Initial_T, \r\n  p3.Last_Name as Insured_Last_Name_T, "
            "p3.Date_of_Birth as Insured_DOB_T, \r\n    "
            "CASE p3.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Insured_Gender_T,"
            "\r\n p3.Street_1 as Insured_Address1_T, p3.Street_2 as Insured_Address2_T, p3.City as Insured_City_T, \r\n   "
            "p3.State as Insured_State_T, p3.Zip_Code as Insured_Zip_T, coalesce(p3.Phone_1, "
            "p3.Contact_Phone_1) as Insured_Phone_T,\r\n /*Falta*/ \"\"  as Authorization_No_T,\r\n  \r\n    "
            "/* - Guarantor - */\r\n CASE when c.Guarantor = c.Chart_Number then \"Self\" "
            "when c.Guarantor = c.Insured_1 then c.Insured_Relationship_1 \r\n  END as Guarantor_Relation_To_Patient, "
            "g.First_Name as Guarantor_First_Name, g.Middle_Initial as Guarantor_Middle_Initial,"
            "\r\n   g.Last_Name as Guarantor_Last_Name, g.Date_of_Birth as Guarantor_DOB,\r\n   "
            "CASE g.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Guarantor_Gender,\r\n  "
            "g.Street_1 as Guarantor_Address1, g.Street_2 as Guarantor_Address2, g.City as Guarantor_City, "
            "g.State as Guarantor_State,\r\n   g.Zip_Code as Guarantor_ZIP, coalesce(g.Phone_1, g.Contact_Phone_1) "
            "as Guarantor_Home_Phone, g.Work_Phone as Guarantor_Work_Phone, \r\n "
            "g.Social_Security_Number as Guarantor_SSN, \r\n /* Guarantors Employer Data */\r\n    "
            "g.Employer as Guarantor_Employer_Name, \"\" as Guarantor_Employer_Address1, \"\" as Guarantor_Employer_Address2, "
            "\r\n   \"\" as Guarantor_Employer_City, \"\" as Guarantor_Employer_State, \"\" as Guarantor_Employer_ZIP, "
            "g.Chart_Number as Guarantor_Account_Number, \r\n g.Language, \r\n    /* NPI for Referring? */\r\n    "
            "c.Referring_Provider as Referring_Provider_NPI, \"\" as Location_External_ID\r\n    \r\n"
            "FROM @TABLEPAT p\r\nINNER JOIN (@TABLEPATCAS pc \r\n            "
            "INNER JOIN (@TABLECAS c\r\n                     "
            "LEFT JOIN @TABLEPAT p1 on p1.Chart_Number = c.Insured_1\r\n                     "
            "LEFT JOIN @TABLEPAT p2 on p2.Chart_Number = c.Insured_2\r\n                     "
            "LEFT JOIN @TABLEPAT p3 on p3.Chart_Number = c.Insured_3\r\n                     "
            "LEFT JOIN @TABLEPAT g on g.Chart_Number = c.Guarantor) \r\n         "
            "ON c.Chart_Number = pc.chart_number AND c.Case_Number = pc.case_number )\r\n "
            "ON pc.chart_number = p.Chart_Number\r\nORDER BY pc.case_number;',concat('AutoGenerated on ',"
            "'"+DATEFORMATTED+"'),now(),now(),1,0);")
        

        for key in files:
            if key=="cas":
                TABLECAS=files[key]
            elif key=="pat":
                TABLEPAT=files[key]
            salida[key]=files[key]
        

        if TABLECAS!="" and TABLEPAT!="":
            TABLEPATCAS="nwpatcas"+str(TS)
            sqlPATCAS = ("CREATE TABLE "+TABLEPATCAS+" AS "
                "SELECT chart_number, MAX(case_number) as case_number, count(*) as many "
                "FROM "+TABLECAS+" GROUP BY chart_number;")

            #ES NECESARIO CREAR TABLEPATCAS RESTANDO A LOS PATCAS PREVIAMENTE CREADOS
            #SELECT table_name FROM information_schema.tables where table_name like 'nwpatcas%';


            sqlReport = sqlReport.replace('@TABLEPATCAS', TABLEPATCAS)
            sqlReport = sqlReport.replace('@TABLEPAT', TABLEPAT)
            sqlReport = sqlReport.replace('@TABLECAS', TABLECAS)
            
            sqlMAXID = "SELECT coalesce(max(id),0)+1 FROM explorer_query;"
            conn = mdb.connect(host=database['HOST'],
                                user=database['USER'],
                                passwd=database['PASSWORD'],
                                db=database['NAME'])
            with conn:
                cur = conn.cursor()
                try:
                    cur.execute(sqlMAXID)
                    MAXID = int(cur.fetchone()[0])
                    sqlReport = sqlReport.replace('@MAXID', str(MAXID))
                    cur.execute(sqlPATCAS)
                    cur.execute(sqlReport)
                    #salida['reporte']=sqlReport
                    #salida['patcas']=sqlPATCAS
                    salida['patcas']=TABLEPATCAS
                except Exception as e:
                    salida = {'error':e}
            return JsonResponse(salida,safe=False)
        else:
            return JsonResponse({'error':'no se recibieron todos los archivos'},safe=False)

        

## maybe the shit below can go away

class BasicVersionCreateView(PictureCreateView):
    template_name_suffix = '_basic_form'


class BasicPlusVersionCreateView(PictureCreateView):
    template_name_suffix = '_basicplus_form'


class AngularVersionCreateView(PictureCreateView):
    template_name_suffix = '_angular_form'


class jQueryVersionCreateView(PictureCreateView):
    template_name_suffix = '_jquery_form'


