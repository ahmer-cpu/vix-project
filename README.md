# vix-project
This project explored the CBOE Volatility Index (VIX) as a hedging instrument on a portfolio of equities. Notebooks:

1) 01_data_collection: Custom scraping scripts for historical data for any ticker, frequency, time period and types on Yahoo Finance. I mainly implemented these using Playwright and Beautiful Soup.
2) 02_visualization: Visualize the data and pre-process it to build intuition for our goals.  
3) 03_event_study: Research market impact on the VIX, detect and explain how real-world news causes VIX spikes.
4) 04_analysis: Perform an initial regression and sensitivity analysis to demonstrate the relationship between SPX and VIX, and motivate our hedging strategy.
5) 05_hedging_portfolio: Conduct the major hedging analysis. We first explain our VIX exposure instrument (Short-term VIX Futures ETF), demonstrate relationships rigorously, perform static/dynamic hedging, and evaluate and explain performances.
