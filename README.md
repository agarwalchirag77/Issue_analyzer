# Issue_analyzer
HevoCX Issue Analyzer 

### Steps to set up:
1. Clone the repo using: 
```
 git clone git@github.com:agarwalchirag77/Issue_analyzer.git
```
3. Create a virtual env and Install the requirements file:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
4. Install cloudflare tunnel to proxy your localhost (Optional)
```
brew install cloudflared
cloudflared tunnel --url http://localhost:8000
```
6. Run the following command to start the server:
```
uvicorn main:app --reload
```
