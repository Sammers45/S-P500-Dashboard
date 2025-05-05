import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
from pandas.tseries.offsets import Day
from snowflake.snowpark.context import get_active_session

import plotly.io as pio

pio.renderers.default = "svg"

session = get_active_session()
## Get active session
#connection_parameters = {
 #       "account": "oeldayc-xe11259",
#        "user": "PATESAMR",
#        "password": "************",
#        "role": "ACCOUNTADMIN",
#        "warehouse": "COMPUTE_WH",
#        "database": "sp500_data",
#        "schema": "finance"
#}

sf = st.secrets["snowflake"]

#session = Session.builder.configs(connection_parameters).create()

# Streamlit UI
st.set_page_config(layout="wide")
st.title('S&P 500 Forecasting & Analysis Dashboard')

# Tabs
# Correct:
forecast_tab, analysis_tab = st.tabs(["Forecasting", "Visual Analytics"])

with forecast_tab:

        valid_symbols_df = session.table("finance.stock_prices") \
                .group_by("symbol").agg(count("close").alias("record_count")) \
                .filter(col("record_count") > 30) \
                .select("symbol").distinct().to_pandas()

        # Get shortnames from companies table
        company_df = session.table("finance.companies") \
                .select("symbol", "shortname") \
                .to_pandas()

        # Merge to include shortnames
        valid_symbols_df = valid_symbols_df.merge(company_df, on="SYMBOL", how="left")

        # Create dropdown display labels
        symbol_options = [f"{row['SYMBOL']} ({row['SHORTNAME']})" for _, row in valid_symbols_df.iterrows()]
        symbol_map = {label: row['SYMBOL'] for label, row in zip(symbol_options, valid_symbols_df.to_dict('records'))}

        # Show dropdown with shortnames
        selected_label = st.selectbox("Choose a company symbol:", symbol_options)
        selected_symbol = symbol_map[selected_label]
        forecast_days = st.slider("Forecast horizon (days):", min_value=7, max_value=60, value=30)
        model_choice = st.selectbox("Choose forecasting model:", ["Prophet", "ARIMA"])

        raw_df = session.table("finance.stock_prices") \
                .filter(f"symbol = '{selected_symbol}'") \
                .select("trade_date", "close") \
                .order_by("trade_date") \
                .to_pandas()

        raw_df.columns = [c.upper() for c in raw_df.columns]
        df = raw_df.rename(columns={"TRADE_DATE": "ds", "CLOSE": "y"})

        if df.dropna().shape[0] < 2:
                st.warning(f"Not enough data for symbol '{selected_symbol}'.")
        else:
                forecast = None
                if model_choice == "Prophet":
                        # Fit and predict
                        model = Prophet(daily_seasonality=True)
                        model.fit(df)
                        future = model.make_future_dataframe(periods=forecast_days)
                        forecast = model.predict(future)

                        # Get the cutoff date (last date from original df)
                        cutoff_date = df['ds'].max()

                        # Plot with annotation
                        fig = model.plot(forecast)
                        plt.axvline(cutoff_date, color='red', linestyle='--', label='Forecast Start')
                        plt.legend()
                        st.pyplot(fig)

                        actual = df["y"].values[-forecast_days:]
                        predicted = forecast["yhat"].values[-forecast_days:]
                        mae = mean_absolute_error(actual, predicted)
                        rmse = np.sqrt(mean_squared_error(actual, predicted))
                        st.markdown(f"**Prophet MAE:** {mae:.2f} | **RMSE:** {rmse:.2f}")

                        result_df = forecast[['ds', 'yhat']].tail(forecast_days).copy()
                        result_df["symbol"] = selected_symbol
                        result_df.rename(columns={'ds': "forecast_date", "yhat": "predicted_close"}, inplace=True)


                elif model_choice == "ARIMA":

                        df.set_index("ds", inplace=True)
                        df.index = pd.to_datetime(df.index)
                        ts = df["y"].asfreq("D").interpolate()

                        # Train-test split
                        train = ts[:-forecast_days]
                        test = ts[-forecast_days:]

                        # Fit ARIMA
                        model = ARIMA(train, order=(5, 1, 0))
                        model_fit = model.fit()
                        arima_forecast = model_fit.forecast(steps=forecast_days)
                        forecast_df = pd.DataFrame({
                                "Date": test.index,  # These are the forecast dates
                                "Actual": test.values,
                                "Forecast": arima_forecast
                        })
                        cutoff_date = test.index[0]  # First forecast date

                        # Plot ARIMA Forecast with cutoff annotation
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=forecast_df["Date"], y=forecast_df["Actual"], mode='lines+markers',
                                                 name='Actual'))
                        fig.add_trace(go.Scatter(x=forecast_df["Date"], y=forecast_df["Forecast"], mode='lines',
                                                 name='Forecast'))
                        fig.add_vline(x=cutoff_date, line_width=2, line_dash="dash", line_color="red")
                        fig.add_annotation(x=cutoff_date, y=max(forecast_df["Actual"]), text="Forecast Starts",
                                           showarrow=True, arrowhead=2)
                        fig.update_layout(title=f"ARIMA Forecast: {selected_symbol}", xaxis_title="Date",
                                          yaxis_title="Price")
                        st.plotly_chart(fig)

                        mae = mean_absolute_error(test, arima_forecast)
                        rmse = np.sqrt(mean_squared_error(test, arima_forecast))
                        st.markdown(f"**ARIMA MAE:** {mae:.2f} | **RMSE:** {rmse:.2f}")

                        result_df = pd.DataFrame({
                                "forecast_date": test.index,
                                "predicted_close": arima_forecast,
                                "symbol": selected_symbol
                        })
                # Export to SnowFlake
        if not result_df.empty:
                snow_df = session.create_dataframe(result_df)
                table_name = f"Forecast_{model_choice}_output"
                snow_df.write.mode('overwrite').save_as_table(f"finance.{table_name}")
                st.success(f"Forecast Results saved to SnowFlake as 'finance.{table_name}'")
with analysis_tab:
        st.header("SQL-Based Visual Analysis")

        df_sector = session.sql("""
            SELECT sp.trade_date, c.sector, AVG(sp.close) AS avg_sector_price
            FROM finance.stock_prices AS sp
            JOIN finance.companies AS c ON sp.symbol = c.symbol
            GROUP BY sp.trade_date, c.sector
        """).to_pandas()

            # Plot with Matplotlib
        fig, ax = plt.subplots(figsize=(12, 6))
        for sector, grp in df_sector.groupby('SECTOR'):
            ax.plot(grp['TRADE_DATE'], grp['AVG_SECTOR_PRICE'], label=sector)
        ax.set_title('Average Sector Price Over Time')
        ax.set_xlabel('Date')
        ax.set_ylabel('Avg Close Price')
        ax.legend(loc='upper left', fontsize='small', ncol=2)
        st.pyplot(fig, use_container_width=True)


        df_tech = session.sql("""
        WITH stock_range AS (
            SELECT symbol, MIN(trade_date) AS start_date, MAX(trade_date) AS end_date
            FROM finance.stock_prices
            GROUP BY symbol
        ),
        stock_prices_summary AS (
            SELECT sr.symbol, c.shortname, sr.start_date, sr.end_date,
                   sp_start.close AS start_price, sp_end.close AS end_price,
                   ROUND(((sp_end.close - sp_start.close) / sp_start.close) * 100, 2) AS percent_change
            FROM stock_range sr
            JOIN finance.companies c ON sr.symbol = c.symbol
            JOIN finance.stock_prices sp_start ON sr.symbol = sp_start.symbol AND sr.start_date = sp_start.trade_date
            JOIN finance.stock_prices sp_end ON sr.symbol = sp_end.symbol AND sr.end_date = sp_end.trade_date
            WHERE c.sector = 'Technology'
        )
        SELECT * FROM stock_prices_summary ORDER BY percent_change DESC LIMIT 10
    """).to_pandas()
        fig2 = px.bar(df_tech, x='SYMBOL', y='PERCENT_CHANGE', color='SHORTNAME', title='Top Tech Stocks by % Change')
        st.plotly_chart(fig2, use_container_width=True)

    
        df_top_sector = session.sql("""
                WITH stock_range AS (
                    SELECT symbol, MIN(trade_date) AS start_date, MAX(trade_date) AS end_date
                    FROM finance.stock_prices
                    GROUP BY symbol
                ),
                stock_growth AS (
                    SELECT c.sector, sr.symbol, c.shortname,
                           sp_start.close AS start_price, sp_end.close AS end_price,
                           ROUND(((sp_end.close - sp_start.close) / sp_start.close) * 100, 2) AS percent_change
                    FROM stock_range sr
                    JOIN finance.companies c ON sr.symbol = c.symbol
                    JOIN finance.stock_prices sp_start ON sr.symbol = sp_start.symbol AND sr.start_date = sp_start.trade_date
                    JOIN finance.stock_prices sp_end ON sr.symbol = sp_end.symbol AND sr.end_date = sp_end.trade_date
                ),
                ranked_growth AS (
                    SELECT *, RANK() OVER (PARTITION BY sector ORDER BY percent_change DESC) AS rnk
                    FROM stock_growth
                )
                SELECT sector, symbol, shortname, start_price, end_price, percent_change
                FROM ranked_growth
                WHERE rnk = 1
            """).to_pandas()
        fig3 = px.bar(df_top_sector, x="SECTOR", y="PERCENT_CHANGE", color="SYMBOL", title="Top Stocks per Sector")
        st.plotly_chart(fig3, use_container_width=True)



        df_ma = session.sql("""
            SELECT symbol,
                trade_date,
                ROUND(close, 3) AS close,
                ROUND(
                    AVG(close) OVER (
                        PARTITION BY symbol 
                        ORDER BY trade_date 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ), 2
                ) AS moving_avg_7d
            FROM finance.stock_prices
            WHERE symbol = 'MSFT'
            ORDER BY trade_date
        """).to_pandas()

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(df_ma['TRADE_DATE'], df_ma['CLOSE'], label='Close')
        ax.plot(df_ma['TRADE_DATE'], df_ma['MOVING_AVG_7D'], label='7‑Day MA')
        ax.set_title('MSFT 7‑Day Moving Average')
        ax.set_xlabel('Date')
        ax.set_ylabel('Price')
        ax.legend()
        st.pyplot(fig, use_container_width=True)



        df_volume = session.sql("""
                SELECT trade_date, SUM(volume) AS total_volume
                FROM finance.stock_prices
                GROUP BY trade_date
                ORDER BY trade_date
            """).to_pandas()
        fig5 = px.area(df_volume, x="TRADE_DATE", y="TOTAL_VOLUME", title="Trading Volume Over Time")
        st.plotly_chart(fig5, use_container_width=True)



        df_cap = session.sql("""
                SELECT sector, ROUND(SUM(market_cap)/1e9, 2) AS total_market_cap
                FROM finance.companies
                GROUP BY sector
            """).to_pandas()
        fig6 = px.pie(df_cap, values="TOTAL_MARKET_CAP", names="SECTOR", title="Sector-wise Market Cap (in Bn)")
        st.plotly_chart(fig6, use_container_width=True)


    
        df_volatility = session.sql("""
                WITH Return AS (
                    SELECT symbol, trade_date,
                           (close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS daily_return
                    FROM finance.stock_prices
                ),
                volatility AS (
                    SELECT symbol, STDDEV(daily_return) AS volatility
                    FROM Return
                    WHERE daily_return IS NOT NULL
                    GROUP BY symbol
                )
                SELECT * FROM volatility WHERE volatility > 0.05 ORDER BY volatility DESC
            """).to_pandas()
        fig7 = px.bar(df_volatility, x="SYMBOL", y="VOLATILITY", title="High Volatility Stocks")
        st.plotly_chart(fig7, use_container_width=True)

session.close()
