# IMF Data Fetcher

\section{IMF Data Fetcher Workflow}

\begin{enumerate}
    \item \textbf{Step 1: Fetch Complete Metadata}
    
    Download complete structural metadata for an entire IMF dataset. This provides all available dimensions, countries, and indicators for that dataset.
    \begin{enumerate}
        \item Run $metadata_downloader$ script from terminal: \\
        \texttt{python scripts/discovery/metadata\_downloader.py}
        \item When prompted choose to download a \textbf{DataStructure} (or CodeList). DataStructure is best.
        \item Enter the \textbf{Dataflow ID} for the dataset you wish to explore. Examples Dataflow IDs:
        \begin{itemize}
            \item \texttt{IFS} - International Financial Statistics (IFS)
            \item \texttt{BOP} - Balance of Payments Statistics (BOP)
            \item \texttt{IL} - International Liquidity (IL)
            \item \texttt{DOT} - Direction of Trade Statistics (DTS)
            \item \texttt{FSI} - Financial Soundness Indicators (FSI)
            \item \texttt{GFSR} - Global Financial Stability Report (GFDR)
        \end{itemize}
        \item Save the file with suggested default name (e.g., \texttt{datastructure\_BOP.json}). This file will be saved in \texttt{scripts/data\_collection/IMF\_datasets/} directory.
    \end{enumerate}
    
    \item \textbf{Step 2: Discover Series Key}
    
    Use $discovery_series$ to interactively explore complete downloaded metadata file. Find specific codes for each dimension to build a full series key.
    \begin{enumerate}
        \item Run discovery script: \\
        \texttt{python scripts/discovery/discover\_series.py}
        \item Select new \texttt{datastructure\_... .json} file downloaded in Step 1
        \item For each dimension presented (e.g., 'COUNTRY', 'INDICATOR'), use \texttt{list} command or search terms to find t specific codes
        \item After making a selection for each dimension, the script will output the final, correctly ordered \textbf{Series Key}. It will look something like: \texttt{M.CN.RXF11FX\_REVS.USD}
    \end{enumerate}
    
    \item \textbf{Step 3: Fetch the Dataset}
    
   Use $IMF_fetcher$ with the information from previous step to download the specific time series data.
    \begin{enumerate}
        \item Run the direct key fetcher script: \\
        \texttt{python scripts/data\_collection/imf\_IMF\_fetcher.py}
        \item Enter \textbf{Dataflow ID} from Step 1 (e.g., `IL`)
        \item Enterfull \textbf{Series Key Filter} discovered in Step 2 (e.g., `M.CN.RXF11FX_REVS.USD`)
        \item Enter desired start and end years.
        \item Provide a descriptive name for the data series (optional)
        \item Script will download specific data and save it as CSV and Parquet files in \texttt{imf\_api\_output\_directkey/} directory
    \end{enumerate}
\end{enumerate}
