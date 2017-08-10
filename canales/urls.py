from django.conf.urls import include, url
from django.http import HttpResponseRedirect
from django.conf import settings
from fileupload.views import ProcessCsvView, PostProcessView

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = [
    url(r'^$', lambda x: HttpResponseRedirect('/upload/new/')),
    url(r'^upload/', include('fileupload.urls')),
    url(r'^explorer/', include('explorer.urls')),
    url(r'^process/', ProcessCsvView.as_view(), name='process'),
    url(r'^postproc/', PostProcessView.as_view(), name='postproc'),
    url(r'^admin/', include(admin.site.urls)),
]

if settings.DEBUG:
    from django.conf.urls.static import static
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
