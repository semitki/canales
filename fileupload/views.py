# encoding: utf-8
import logging
import json

from django.http import HttpResponse, JsonResponse
from django.views.generic import CreateView, DeleteView, ListView, View
from .models import Picture
from .response import JSONResponse, response_mimetype
from .serialize import serialize
import requests


logger = logging.getLogger(__name__)

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
                return self._monitor(file_id = file_id,
                        response = json.loads(r.text)['Status'])
        else:
            return {'error':
                    'Error monitoring file in sqlizer'}

    def get(self, request, *args, **kwargs):
        resource_id = request.path.split('/')[2]
        csv = Picture.objects.get(pk = resource_id)
        ts = csv.timestamp.strftime("%Y%m%d%H%M%S")
        data = {'DatabaseType': 'SQLServer',
            'FileType': 'csv',
            'FileName': csv.slug,
            'TableName': 'nw' + csv.file_type + ts,
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
                    response = self._monitor(file_id)
                else:
                    response = {'error':
                            'Error trying to finalize file upload to sqlizer'}
            else:
                response = {'error':
                        'Error trying to upload file to sqlizer'}
        else:
            response = {'error':
                    'Error trying to initate file conversion on sqlizer'}

        return JsonResponse(response, safe=False)


## maybe the shit below can go away

class BasicVersionCreateView(PictureCreateView):
    template_name_suffix = '_basic_form'


class BasicPlusVersionCreateView(PictureCreateView):
    template_name_suffix = '_basicplus_form'


class AngularVersionCreateView(PictureCreateView):
    template_name_suffix = '_angular_form'


class jQueryVersionCreateView(PictureCreateView):
    template_name_suffix = '_jquery_form'


