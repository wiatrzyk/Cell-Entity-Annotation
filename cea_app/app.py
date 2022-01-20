from PyQt5 import QtCore, QtWidgets
from annotation import *
from pandas import DataFrame
import time

class Worker(QtCore.QObject):
    finished = QtCore.pyqtSignal(bool)
    save_file = QtCore.pyqtSignal(str, DataFrame)
    progress = QtCore.pyqtSignal(int)
    app_shared = dict()
    check = bool
    lookup = bool
    spootlight = bool
    done = False

    def run(self):
        tables_folder_path = self.app_shared['folderPath']
        targets_file_path = self.app_shared['targets']

        data = read_targets(targets_file_path, tables_folder_path)
        data['text'] = data['text'].map(clear_entity)
        result_df = data.assign(annotation=None, candidates=None)
        
        url_check_results = result_df.copy()
        dbpedia_lookup_results_df = result_df.copy()
        spotlight_lookup_results_df = result_df.copy()
        
        for index, row in result_df.iterrows():
            cell = row['text']
            if not isinstance(cell, str):
                continue
            
            result_candidates = []
            if self.check:
                # CHECK
                url = check_url(cell)
                if url is not None:
                    result_candidates.append(url)
                    get_results(cell, index, [url], url_check_results)
            
            if self.lookup:
                # DBPEDIA
                dbpedia_lookup_urls = dbpedia_lookup(cell, 10)
                if dbpedia_lookup_urls is not None:
                    for url in dbpedia_lookup_urls:
                        result_candidates.append(url)
                    get_results(cell, index, dbpedia_lookup_urls, dbpedia_lookup_results_df)
                    
            if self.spootlight:
                # SPOTLIGHT
                spotlight_lookup_urls = spotlight_lookup(cell)
                if spotlight_lookup_urls is not None:
                    for url in spotlight_lookup_urls:
                        if url not in result_candidates:
                            result_candidates.append(url)
                    get_results(cell, index, spotlight_lookup_urls, spotlight_lookup_results_df)

            get_results(cell, index, result_candidates, result_df)

            self.progress.emit(int((index+1)/result_df.shape[0] * 100))

        if(self.check):
            self.save_file.emit("check", url_check_results)
            while not self.done:
                time.sleep(1)
        if(self.lookup):
            self.save_file.emit("lookup", dbpedia_lookup_results_df)
            while not self.done:
                time.sleep(1)
        if(self.spootlight):
            self.save_file.emit("spootlight", spotlight_lookup_results_df)
            while not self.done:
                time.sleep(1)
        
        self.save_file.emit("final", spotlight_lookup_results_df)
        while not self.done:
            time.sleep(1)
        self.finished.emit(True)


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(442, 150)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.progressBar = QtWidgets.QProgressBar(self.centralwidget)
        self.progressBar.setGeometry(QtCore.QRect(120, 90, 280, 20))
        self.progressBar.setProperty("value", 0)
        self.progressBar.setObjectName("progressBar")
        self.checkBox = QtWidgets.QCheckBox(self.centralwidget)
        self.checkBox.setGeometry(QtCore.QRect(20, 70, 70, 17))
        self.checkBox.setObjectName("checkBox")
        self.checkBox_2 = QtWidgets.QCheckBox(self.centralwidget)
        self.checkBox_2.setGeometry(QtCore.QRect(20, 110, 70, 17))
        self.checkBox_2.setObjectName("checkBox_2")
        self.checkBox_3 = QtWidgets.QCheckBox(self.centralwidget)
        self.checkBox_3.setGeometry(QtCore.QRect(20, 90, 70, 17))
        self.checkBox_3.setObjectName("checkBox_3")
        self.pushButton = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton.setGeometry(QtCore.QRect(30, 10, 120, 40))
        self.pushButton.setObjectName("pushButton")
        self.pushButton_2 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_2.setGeometry(QtCore.QRect(160, 10, 120, 40))
        self.pushButton_2.setObjectName("pushButton_2")
        self.pushButton_3 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_3.setGeometry(QtCore.QRect(290, 10, 120, 40))
        self.pushButton_3.setObjectName("pushButton_3")
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 442, 21))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.shared = {}
        self.done = False
        self.pushButton.clicked.connect(self.__load_folder)
        self.pushButton_2.clicked.connect(self.__load_file)
        self.pushButton_3.clicked.connect(self.__run_annotation)

    def __load_file(self):
        filename = self.openFileNameDialog()

    def __load_folder(self):
        folder_name = self.openFolderDialog()

    def __run_annotation(self):
        if 'targets' in self.shared.keys() and 'folderPath' in self.shared.keys():
            self.thread = QtCore.QThread()
            self.worker = Worker()
            self.worker.moveToThread(self.thread)

            self.thread.started.connect(self.worker.run)
            self.worker.finished.connect(self.thread.quit)
            self.worker.finished.connect(self.worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)
        
            self.worker.progress.connect(self.reportProgress)
            self.worker.app_shared = self.shared
            self.worker.check = self.checkBox.isChecked()
            self.worker.lookup = self.checkBox_2.isChecked()
            self.worker.spootlight = self.checkBox_3.isChecked()
            self.worker.save_file.connect(self.save_result)
            self.worker.done = self.done

            self.thread.start()
            self.app_ready(False)
            self.thread.finished.connect(self.app_ready)

    def app_ready(self, ready=True):
        self.pushButton.setEnabled(ready)
        self.pushButton_2.setEnabled(ready)
        self.pushButton_3.setEnabled(ready)
        self.checkBox.setEnabled(ready)
        self.checkBox_2.setEnabled(ready)
        self.checkBox_3.setEnabled(ready)
        self.progressBar.setValue(0)
        self.done = not ready

    def reportProgress(self, value):
        self.progressBar.setValue(int(value))

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Cell Entity Annotation"))
        self.checkBox.setText(_translate("MainWindow", "URL"))
        self.checkBox_2.setText(_translate("MainWindow", "Lookup"))
        self.checkBox_3.setText(_translate("MainWindow", "Spootlight"))
        self.pushButton.setText(_translate("MainWindow", "Load folder with tables"))
        self.pushButton_2.setText(_translate("MainWindow", "Load targets"))
        self.pushButton_3.setText(_translate("MainWindow", "Annotate"))
    
    def openFolderDialog(self):
        options = QtWidgets.QFileDialog.Options()
        options |= QtWidgets.QFileDialog.DontUseNativeDialog
        folderPath = QtWidgets.QFileDialog.getExistingDirectory(None, 'Select Folder')
        if folderPath:
            self.shared['folderPath'] = folderPath
            return folderPath
    
    def openFileNameDialog(self):
        options = QtWidgets.QFileDialog.Options()
        options |= QtWidgets.QFileDialog.DontUseNativeDialog
        fileName, _ = QtWidgets.QFileDialog.getOpenFileName(None, "Choose file", "", "CSV files(*.csv);;All Files (*)", options=options)
        if fileName:
            self.shared['targets'] = fileName
            return fileName

    def save_result(self, an_type, dataframe):
        options = QtWidgets.QFileDialog.Options()
        options |= QtWidgets.QFileDialog.DontUseNativeDialog
        fileName, _ = QtWidgets.QFileDialog.getSaveFileName(None, "Save results", f"{an_type}_results.csv", "CSV files(*.csv)", options=options)
        if fileName:
            dataframe.to_csv(fileName, sep=',', index=False)
        self.worker.done = True

if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())