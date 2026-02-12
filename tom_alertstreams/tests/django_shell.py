#!/usr/bin/env python

from django.core.management import call_command
from boot_django import boot_django


# This code runs a django shell as though you had run `manage.py shell`
boot_django()
call_command('shell_plus')
