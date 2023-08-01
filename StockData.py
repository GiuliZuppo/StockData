{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "dce80221",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "                      1. open   2. high    3. low  4. close 5. volume\n",
      "Fecha                                                                \n",
      "2023-07-31 19:55:00  336.0000  336.1200  336.0000  336.0200      2919\n",
      "2023-07-31 19:50:00  336.0500  336.0500  335.9900  336.0100      1796\n",
      "2023-07-31 19:45:00  336.0200  336.1000  336.0000  336.1000       572\n",
      "2023-07-31 19:40:00  335.9700  336.0900  335.9600  336.0000      1714\n",
      "2023-07-31 19:35:00  335.9500  335.9700  335.9100  335.9700      2274\n",
      "...                       ...       ...       ...       ...       ...\n",
      "2023-07-31 12:00:00  334.3400  334.6500  334.3100  334.6300    171398\n",
      "2023-07-31 11:55:00  333.8300  334.4670  333.8200  334.3380    186296\n",
      "2023-07-31 11:50:00  334.5550  334.6000  333.6400  333.8400    337422\n",
      "2023-07-31 11:45:00  334.1300  334.7800  334.0600  334.5660    208472\n",
      "2023-07-31 11:40:00  333.9750  334.2500  333.8800  334.1300    314951\n",
      "\n",
      "[100 rows x 5 columns]\n",
      "DataFrame guardado en stock_data.csv\n"
     ]
    }
   ],
   "source": [
    "import requests\n",
    "import pandas as pd\n",
    "\n",
    "url = \"https://alpha-vantage.p.rapidapi.com/query\"\n",
    "\n",
    "querystring = {\n",
    "    \"interval\": \"5min\",\n",
    "    \"function\": \"TIME_SERIES_INTRADAY\",\n",
    "    \"symbol\": \"MSFT\",\n",
    "    \"datatype\": \"json\",\n",
    "    \"output_size\": \"compact\"\n",
    "}\n",
    "\n",
    "headers = {\n",
    "    \"X-RapidAPI-Key\": \"1f26e6b29bmsh13204e57bfb7c06p19c023jsn9ebc5651627b\",\n",
    "    \"X-RapidAPI-Host\": \"alpha-vantage.p.rapidapi.com\"\n",
    "}\n",
    "\n",
    "response = requests.get(url, headers=headers, params=querystring)\n",
    "\n",
    "if response.status_code == 200:\n",
    "    data_json = response.json()\n",
    "\n",
    "    # Obtener la serie de tiempo de los precios\n",
    "    time_series_data = data_json.get(\"Time Series (5min)\", {})\n",
    "\n",
    "    # Crear el DataFrame\n",
    "    df = pd.DataFrame.from_dict(time_series_data, orient=\"index\")\n",
    "\n",
    "    # Asignar un nombre a la primera columna porque estaba vacia\n",
    "    df.index.name = 'Fecha'\n",
    "\n",
    "    # Mostrar el DataFrame \n",
    "    print(df)\n",
    "\n",
    "    # Guardar el DataFrame en un archivo CSV\n",
    "    stockdata = \"stock_data.csv\"\n",
    "    df.to_csv(stockdata)\n",
    "    print(f\"DataFrame guardado en {stockdata}\")\n",
    "else:\n",
    "    print(f\"Error al hacer la solicitud a la API. Código de estado: {response.status_code}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "b9ecc6a4",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
