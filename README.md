![header-cheetah](https://user-images.githubusercontent.com/79997186/184224088-de4f3003-0c22-4a17-8cc7-b341b8e5b55d.png)

&nbsp;
&nbsp;
&nbsp;

## Introduction

This repository hosts the Terminal Link Plugin Integration with the QuantConnect LEAN Algorithmic Trading Engine and the Bloomberg Desktop API. LEAN is a brokerage agnostic operating system for quantitative finance. Thanks to open-source plugins such as this [LEAN](https://github.com/QuantConnect/Lean) can route strategies to almost any market.

[LEAN](https://github.com/QuantConnect/Lean) is maintained primarily by [QuantConnect](https://www.quantconnect.com), a US based technology company hosting a cloud algorithmic trading platform. QuantConnect has successfully hosted more than 200,000 live algorithms since 2015, and trades more than $1B volume per month.

### About Terminal Link

<p align="center">
<picture >
 <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/79997186/188236972-6edc561d-e944-4ed5-a236-56461dd78d41.png">
 <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/79997186/188236978-4316dd6e-cfb7-43c7-a058-e70009396cd0.png">
 <img alt="introduction" width="40%">
</picture>
<p>

LEAN can connect to the Bloomberg™ Desktop API (DAPI) through our Terminal Link plug-in. This product is in no way affiliated with or endorsed by Bloomberg™; it is simply an add-on. Add Terminal link to your organization to access the 1,300+ prime brokerages in the Bloomberg Execution Management System network.

For more information about Terminal Link, see [Prime Brokerages](https://www.quantconnect.com/docs/v2/our-platform/live-trading/brokerages/trading-technologies). 

### Installation:

Install into the same root folder as /Lean, so the dependencies will link with relative pathing.
  ```
  /Lean 
  /LeanTerminalLink
  ```


## Using the Brokerage Plugin
 
  
### Deploying Terminal Link with LEAN CLI

Follow these steps to start local live trading with Terminal Link:

1.  Open a terminal in your [CLI root directory](https://www.quantconnect.com/docs/v2/lean-cli/initialization/directory-structure#02-lean-init).
2.  Run `lean live "<projectName>"` to start a live deployment wizard for the project in `./<projectName>` and then enter the brokerage number.

	```
    $ lean live "My Project"
    Select a brokerage:
    1) Paper Trading
    2) Interactive Brokers
    3) Tradier
    4) OANDA
    5) Bitfinex
    6) Coinbase Pro
    7) Binance
    8) Zerodha
    9) Samco
    10) Terminal Link
    11) Atreyu
    12) Trading Technologies
    13) Kraken
    14) FTX 
    Enter an option: 
	```


3.  Enter the number of the organization that has a subscription for the Terminal Link module. 

    ```
    $ lean live "My Project"
    Select the organization with the Terminal Link module subscription:
    1) Organization 1
    2) Organization 2
    3) Organization 3
    Enter an option: 1
    ```

4.  Enter the environment to use.

    ```
    $ lean live "My Project"
    Environment (Production, Beta): Production
    ```

5.  Enter the host and port of the Bloomberg server.

    ```
    $ lean live "My Project"
    Server host: 127.0.0.1
    Server port: 8194
    ```

6.  Enter the path to the symbol map file.

    ```
    $ lean live "My Project"
    Path to symbol map file: ~/Documents/symbol-map-file.json
    ```

    The symbol map file must be a JSON file (comments are supported) in which the keys are the Bloomberg symbol names and the values are the corresponding QuantConnect symbols. This content can be used as an example:

    ```
    /* This is a manually created file that contains mappings from Bloomberg's own naming to original symbols defined by respective exchanges. */
    {
        // Example:
        /*"SPY US Equity": {
            "Underlying": "SPY",
            "SecurityType": "Equity",
            "Market": "usa"
        }*/
    }
    ```

7.  Enter your EMSX configuration (properties followed by [] can be skipped by pressing enter).

    ```
    $ lean live "My Project"
    EMSX broker: someValue
    EMSX user timezone [UTC]:
    EMSX account []:
    EMSX strategy []:
    EMSX notes []:
    EMSX handling []:
    ```

8.  Enter whether modification must be allowed.

    ```
    $ lean live "My Project"
    Allow modification (yes/no): no
    ```

9.  Enter the number of the data feed to use and then follow the steps required for the data connection.

    ```
    $ lean live "My Project"
    Select a data feed:
    1) Interactive Brokers
    2) Tradier
    3) Oanda
    4) Bitfinex
    5) Coinbase Pro
    6) Binance
    7) Zerodha
    8) Samco
    9) Terminal Link
    10) Trading Technologies
    11) Kraken
    12) FTX
    13) IQFeed
    14) Polygon Data Feed
    15) Custom data only
    To enter multiple options, separate them with comma.:
    ```

    If you select IQFeed, see [IQFeed](https://www.quantconnect.com/docs/v2/lean-cli/live-trading/other-data-feeds/iqfeed) for set up instructions.  
    If you select Polygon Data Feed, see [Polygon](https://www.quantconnect.com/docs/v2/lean-cli/live-trading/other-data-feeds/polygon) for set up instructions.

10.  View the result in the `<projectName>/live/<timestamp>` directory. Results are stored in real-time in JSON format. You can save results to a different directory by providing the `--output <path>` option in step 2.

If you already have a live environment configured in your Lean configuration file, you can skip the interactive wizard by providing the `--environment <value>` option in step 2. The value of this option must be the name of an environment which has `live-mode` set to `true`.

## Order Types and Asset Classes

The following table describes the available order types for each asset class that Terminal Link supports:

| Order Type  | Crypto | Equity | Equity Options | Futures | Futures Options |
| ----------- | ------ | ------ | ------ | ------ | ------ |
| `MarketOrder` | Yes | Yes | Yes | Yes | Yes |
| `LimitOrder` | Yes | Yes | Yes | Yes | Yes |
| `StopMarketOrder` | Yes | Yes | Yes | Yes | Yes |
| `StopLimitOrder` | Yes | Yes | Yes | Yes | Yes |


## Downloading Data

For local deployment, the algorithm needs to download the following datasets:

- [US Equities Security Master](https://www.quantconnect.com/datasets/quantconnect-us-equity-security-master) provided by QuantConnect
- [US Coarse Universe](https://www.quantconnect.com/datasets/quantconnect-us-coarse-universe-constituents)
- [US Futures Security Master](https://www.quantconnect.com/datasets/quantconnect-us-futures-security-master)


## Fees

Orders filled with Terminal Link are subject to the fees of the Bloomberg Execution Management System.


## Reference links:

- BLPAPI documentation: https://bloomberg.github.io/blpapi-docs/
- BLPAPI Core Developer Guide: https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf
- BLPAPI Core User Guide: https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-User-Guide.pdf
- EMSX API documentation: https://emsx-api-doc.readthedocs.io/en/latest/introduction.html
- SDK downloads: https://www.bloomberg.com/professional/support/api-library/
- API emulator: https://github.com/Robinson664/bemu

&nbsp;
&nbsp;
&nbsp;

![whats-lean](https://user-images.githubusercontent.com/79997186/184042682-2264a534-74f7-479e-9b88-72531661e35d.png)

&nbsp;
&nbsp;
&nbsp;

LEAN Engine is an open-source algorithmic trading engine built for easy strategy research, backtesting, and live trading. We integrate with common data providers and brokerages, so you can quickly deploy algorithmic trading strategies.

The core of the LEAN Engine is written in C#, but it operates seamlessly on Linux, Mac and Windows operating systems. To use it, you can write algorithms in Python 3.8 or C#. QuantConnect maintains the LEAN project and uses it to drive the web-based algorithmic trading platform on the website.

## Contributions

Contributions are warmly very welcomed but we ask you to read the existing code to see how it is formatted, commented and ensure contributions match the existing style. All code submissions must include accompanying tests. Please see the [contributor guide lines](https://github.com/QuantConnect/Lean/blob/master/CONTRIBUTING.md).

## Code of Conduct

We ask that our users adhere to the community [code of conduct](https://www.quantconnect.com/codeofconduct) to ensure QuantConnect remains a safe, healthy environment for
high quality quantitative trading discussions.

## License Model

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You
may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
