#FROM tensorflow/tensorflow:latest-gpu-py3-jupyter
#FROM tensorflow/tensorflow:2.9.0rc1-gpu-jupyter
FROM pytorch/pytorch:latest

RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install --no-install-recommends -y openjdk-11-jre-headless

RUN python3 -m pip install --upgrade pip
RUN pip install mlflow[extras] mlflow-torchserve
RUN pip install torch torchvision torchaudio pandas seaborn matplotlib jupyter jupyterlab 
RUN pip install SQLAlchemy xlrd openpyxl
RUN pip install requests flask fastapi
#RUN pip install scikit-learn lightgbm xgboost statsmodels pmdarima prophet darts shap


EXPOSE 8888

CMD jupyter-lab --ip=0.0.0.0 --allow-root
