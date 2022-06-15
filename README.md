# Miso electricity price forecast

## Intro
This project builds out a template for building a CNN time series forecasting model using Pytorch and MLFlow.  Custom data loaders are build to create three input tensors for the CNN model.  An MLFlow experiment tests different model configurations and the best model is served via `torchserve` and custom model handler file was built to format the data from prediction requests.

## Data
Historical wind and solar generation, loads, and prices for the Miso Northern are gathered and stored in a SQLite database.  Three tensors are used for forecasting:

* `hist_future` contains 24 hours of historical values and 24 hours of future values.  This tensor includes wind and solar generation and loads.  While this exercise uses actual future values, in practice these values would come from forecasts.
* `hist` contains 24 hours of price data.  The thought here is that patterns between recent historical prices, wind and solar generation, and loads can be used to forecast the next 24 hours of prices.
* `tabular` contains one-hot-encoded time data, like hours, days, and months.

These three datasets are used to forecast then next 24 hours of prices.  The `get_data.py` file pulls in data from Miso and uploads the data to SQLite.

## CNN model
Pytorch is used to build the neural network.  The general structure is:

* Layers of 1d convoluational blocks, batch normalization, and relu activations for the `hist_future` and `hist` tensors.  The number of layers can be parameterized as well as the kernel sizes and number of outputs channels for each layer.
* The convolutional layers are flattened and concated with the `tabular` tensor.
* The concatenated layers are then feed through fully connected layers.  The number and size of each layer is also parementerized.


## MLFlow
For this experiment different fully connected layer configurations are varied.  The model are also logged during training at 5th epoch.

## Tensorserve
The best model from the MLFlow experiment is packaged with the custom `model)handler.py` file and deployed to a local server.  Packaging the model and examples of converting data to json for the api call and formatting the response are shown in the `build_mlflow_exp.ipynb`.

## Docker
All of this example can run using Docker.  The docker folder contains a docker file and instructions for building and starting up an interactive container in jupyter lab.
