# custom handler file

# model_handler.py

"""
ModelHandler defines a custom model handler.
"""

import torch
from ts.torch_handler.base_handler import BaseHandler
import json
import os

class ModelHandler(BaseHandler):
    """
    A custom model handler implementation.
    """

    def __init__(self):
        self._context = None
        self.initialized = False
        self.explain = False
        self.target = 0
        self.model = None
        self.device = None

    def initialize(self, context):
        """
        Initialize model. This will be called during model loading time
        :param context: Initial context contains model server system properties.
        :return:
        """
        self._context = context
        self.initialized = True
        #  load the model
        self.manifest = context.manifest

        properties = context.system_properties
        model_dir = properties.get("model_dir")
        # self.device = torch.device("cuda:" + str(properties.get("gpu_id")) if torch.cuda.is_available() else "cpu")
        self.device = "cpu"

        # Read model serialize/pt file
        serialized_file = self.manifest['model']['serializedFile']
        model_pt_path = os.path.join(model_dir, serialized_file)
        if not os.path.isfile(model_pt_path):
            raise RuntimeError("Missing the model.pt file")

        self.model = torch.jit.load(model_pt_path)
        self.model = self.model.double().to(self.device)

        self.initialized = True

    def preprocess(self, post_data):
        """
        Transform raw input into model input data.
        :param batch: list of raw requests, should match batch size
        :return: list of preprocessed model input data
        """
        # Take the input data and make it inference ready
        # print(post_data[0]['post_data'])
        preprocessed_data = json.loads(post_data[0]['post_data']) # returns string
        preprocessed_data = json.loads(preprocessed_data) # returns dict
        # print(preprocessed_data)

        for k in preprocessed_data.keys():
            preprocessed_data[k] = torch.asarray(preprocessed_data[k], dtype=torch.float64).to(self.device)
        
        print(preprocessed_data)
        
        return preprocessed_data


    def inference(self, model_input):
        """
        Internal inference methods
        :param model_input: dict of transformed model input data
        :return: list of inference output in NDArray
        """
        # Do some inference call to engine here and return output
        model_output = self.model.forward(**model_input)
        return model_output

    def postprocess(self, inference_output):
        """
        Return inference result.
        :param inference_output: list of inference output
        :return: list of predict results
        """
        # Take output from network and post-process to desired format
        postprocess_output = inference_output
        # postprocess_output = {'prices':postprocess_output.tolist()}
        postprocess_output = postprocess_output.tolist()
        print('output')
        print(postprocess_output)
        
        return postprocess_output

    def handle(self, data, context):
        """
        Invoke by TorchServe for prediction request.
        Do pre-processing of data, prediction using model and postprocessing of prediciton output
        :param data: Input data for prediction
        :param context: Initial context contains model server system properties.
        :return: prediction output
        """
        model_input = self.preprocess(data)
        model_output = self.inference(model_input)
        return self.postprocess(model_output)