# -*- coding: utf-8 -*-
# Generated by Django 1.11.4 on 2017-08-14 14:34
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('fileupload', '0005_auto_20170813_0909'),
    ]

    operations = [
        migrations.AlterField(
            model_name='picture',
            name='slug',
            field=models.SlugField(max_length=255),
        ),
    ]
