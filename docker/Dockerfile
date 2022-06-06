#FROM tensorflow/tensorflow:latest-gpu-py3-jupyter
#FROM tensorflow/tensorflow:2.9.0rc1-gpu-jupyter
FROM pytorch/pytorch:latest

#RUN apt update
#RUN apt install curl

RUN python3 -m pip install --upgrade pip
RUN pip install mlflow[extras]
RUN pip install torch torchvision torchaudio pandas seaborn matplotlib jupyter jupyterlab 
RUN pip install SQLAlchemy xlrd openpyxl
RUN pip install requests flask fastapi
#RUN pip install scikit-learn lightgbm xgboost statsmodels pmdarima prophet darts shap


EXPOSE 8888

CMD jupyter-lab --ip=0.0.0.0 --allow-root
