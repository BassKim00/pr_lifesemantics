# pr_lifesemantics


virtualen 설치 및 가상환경 접속

//이걸 하면(venv)가 경로앞에 표시됨

virtualenv -p python3 venv

source ~venv/bin/activate

//필요한 라이브러리 설치

pip install requirements.txt

//모델 마이그레이션 (디비 클라우드에 올려야 할듯 한번에 끝내려면)

python manage.py migrate

