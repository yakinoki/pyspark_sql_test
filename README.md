# pyspark_sql_test

![ソースコードサイズ](https://img.shields.io/github/languages/code-size/yakinoki/pyspark_sql_test)

[PySparkの環境構築]

PySparkの裏側ではJavaが動いているのでJavaをインストールする。

Sparkの公式ページからApache Sparkをダウンロードする。

Windowsで動かすにはwinutilsが必要なので、以下よりダウンロードする。winutilsは、Apache Spark展開後のbin配下に格納する。

　https://github.com/steveloughran/winutils

環境変数を設定する。コントロールパネルから「システムとセキュリティ」を選択。「システム」をクリックし、「システムの詳細設定」を検索し、「環境変数」をクリック。

* HADOOP_HOME：C:\spark-2.4.5-bin-hadoop2.7

* PATH：$HADOOP_HOME\binを追加

spark-shellコマンドを実行し起動確認する。

### test_code.py
In this file, some code of basic operations for table data are written.
