from django.urls import path
from .views import UploadCSVFileView

urlpatterns = [
    path('', UploadCSVFileView.as_view(), name='upload'),

]