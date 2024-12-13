## API 실행 및 사용법
- api spec 조회   
  -  Swagger: http://localhost:8000/docs
  -  ReDoc: http://localhost:8000/redoc
<br/><br/>
- 실행
  
  ```bash
  # gcp 인스턴스 > hortonworks 도커 컨테이너 기준
  cd /api-server   

  # 개발 시 (포트 포워딩 8000번 추가)
  python3.6 -m uvicorn main:app --reload

  # 배포 시
   nohup python3.6 -u -m gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app > app.log 2>&1 &
  ```