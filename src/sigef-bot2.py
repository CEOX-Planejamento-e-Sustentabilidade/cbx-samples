import webbrowser
import subprocess

def is_firefox_running():
    try:
        subprocess.check_output(['pgrep', '-f', 'firefox'])
        return True
    except subprocess.CalledProcessError:
        return False
    
if not is_firefox_running():    
    firefox_path = '/usr/bin/firefox'  # Replace with the actual path to your Firefox executable
    webbrowser.register('firefox', None, webbrowser.BackgroundBrowser(firefox_path))

webbrowser.get('firefox').open('https://sso.acesso.gov.br/authorize?response_type=code&client_id=sigef.incra.gov.br&redirect_uri=https%3A%2F%2Fsigef.incra.gov.br%2F&scope=openid+email+profile+govbr_confiabilidades+govbr_recupera_certificadox509&state=zsT07dMcXdkRdTe5V0PIDDN2BRAGoJ')
