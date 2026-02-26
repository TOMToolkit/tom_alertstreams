# boot_django.py
#
# This file sets up and configures Django. It's used by scripts that need to
# execute as if running in a Django server.

import os
import django
from django.conf import settings

APP_NAME = 'tom_alertstreams'  # the stand-alone app we are testing

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), APP_NAME))


def boot_django():
    settings.configure(
        BASE_DIR=BASE_DIR,
        # SECURITY WARNING: keep the secret key used in production secret!
        SECRET_KEY='v5j-rg7sc+leg-m+vf947vi34+fs1%+$m%*l%sb7^fnwb$-29y',
        DEBUG=True,
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
            }
        },
        INSTALLED_APPS=(
            'django.contrib.admin',
            'django.contrib.auth',
            'django.contrib.contenttypes',
            'django.contrib.sessions',
            'django.contrib.messages',
            'django.contrib.staticfiles',
            'django.contrib.sites',
            'django_extensions',
            APP_NAME,  # defined above
        ),
        EXTRA_FIELDS={},
        TIME_ZONE='UTC',
        USE_TZ=True,
        MIDDLEWARE=[
            'django.middleware.security.SecurityMiddleware',
            'django.contrib.sessions.middleware.SessionMiddleware',
            'django.middleware.common.CommonMiddleware',
            'django.middleware.csrf.CsrfViewMiddleware',
            'django.contrib.auth.middleware.AuthenticationMiddleware',
            'django.contrib.messages.middleware.MessageMiddleware',
            'django.middleware.clickjacking.XFrameOptionsMiddleware',
        ],
        TEMPLATES=[
            {
                'BACKEND': 'django.template.backends.django.DjangoTemplates',
                'DIRS': [os.path.join(BASE_DIR, 'templates')],
                'APP_DIRS': True,
                'OPTIONS': {
                    'context_processors': [
                        'django.template.context_processors.debug',
                        'django.template.context_processors.request',
                        'django.contrib.auth.context_processors.auth',
                        'django.contrib.messages.context_processors.messages',
                    ],
                },
            },
        ],
        AUTHENTICATION_BACKENDS=(
            'django.contrib.auth.backends.ModelBackend',
        ),
        AUTH_STRATEGY='READ_ONLY',
        STATIC_URL='/static/',
        STATIC_ROOT=os.path.join(BASE_DIR, '_static'),
        STATICFILES_DIRS=[os.path.join(BASE_DIR, 'static')],
        MEDIA_ROOT=os.path.join(BASE_DIR, 'data'),
        MEDIA_URL='/data/',
    )
    django.setup()
