# -*- coding: utf-8 -*-
"""
Created on Fri May 30 15:22:47 2025

@author: Tanner
"""

import init
import sys
import pandas as pd
from qtpy.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QListWidgetItem, QStyledItemDelegate,
    QLabel, QComboBox, QListWidget, QTableWidget, QTableWidgetItem, QPushButton, QSizePolicy,
    QMessageBox, QSplitter, QAbstractItemView, QFormLayout, QScrollArea, QFrame, QGroupBox, QHeaderView
)
from qtpy.QtCore import Qt, QSettings
from qtpy.QtGui import QColor, QFont
import db_access
from contextlib import contextmanager
import traceback
from datetime import datetime, date
import time

# %% Helper Methods

def wrap_layout(layout):
    container = QWidget()
    container.setLayout(layout)
    return container

@contextmanager
def handle_db_errors(parent=None, title="Database Connection Error", message="The following error occurred when trying to connect to the database:"):
    success = True
    try:
        yield lambda: success
    except Exception as e:
        error_msg = str(e) or traceback.format_exc()
         
        QMessageBox.warning(parent, title, 
                            '{}\n\n{}\n\nMake sure you are connected to the Campus VPN and try again.'.format(message, error_msg))
        
        success = False
        
def difference_in_years_and_days(start_date, end_date):
    # Calculate full years
    years = end_date.year - start_date.year

    # Create a tentative anniversary date
    anniversary = date(start_date.year + years, start_date.month, start_date.day)

    # If anniversary is after the end date, subtract one year
    if anniversary > end_date:
        years -= 1
        anniversary = date(start_date.year + years, start_date.month, start_date.day)

    # Days between anniversary and end date
    remaining_days = (end_date - anniversary).days

    return years, remaining_days

class PaddingDelegate(QStyledItemDelegate):
    def __init__(self, h_padding=0, v_padding=0, parent=None):
        super().__init__(parent)
        self.h_padding = h_padding
        self.v_padding = v_padding

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        size.setWidth(size.width() + self.h_padding)
        size.setHeight(size.height() + self.v_padding)
        return size


# %% App Code

class BehaviorTracker(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Behavior Tracker')
        self.settings = QSettings('hankslab', 'Behavior Tracker')
        self.resize(1900, 1000)

        #self.setGeometry(350, 50, 1100, 500)

        # State
        self.filter_categories = ['Protocol', 'Rig', 'Experimenter']
        self.selected_filter = None
        self.category_list = []
        self.selected_category = None
        self.subj_list = []
        self.selected_subjs = []
        self.sess_data = pd.DataFrame()
        self.selected_sess = None

        # UI setup
        self.init_ui()
        self.restore_state()

    def init_ui(self):
        # main container will be a splitter to allow for resizing of panels
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Add margins around central widget
        wrapper_widget = QWidget()
        wrapper_layout = QHBoxLayout(wrapper_widget)
        wrapper_layout.setContentsMargins(15,10,15,10)
        wrapper_layout.addWidget(self.main_splitter)
        self.setCentralWidget(wrapper_widget)
        
        shm = 3 #splitter handle margin
        category_layout = QVBoxLayout()
        category_layout.setContentsMargins(0, 0, shm, 0)
        subj_layout = QVBoxLayout()
        subj_layout.setContentsMargins(shm, 0, shm, 0)
        sess_layout = QVBoxLayout()
        sess_layout.setContentsMargins(shm, 0, shm, 0)
        settings_layout = QVBoxLayout()
        settings_layout.setContentsMargins(shm, 0, 0, 0)

        # filter category panel
        self.category_dropdown = QComboBox()
        self.category_dropdown.addItems(self.filter_categories)
        self.category_dropdown.currentIndexChanged.connect(self.filter_category_changed)
        self.category_dropdown.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        bold_font = self.category_dropdown.font()
        bold_font.setBold(True)
        self.category_dropdown.setFont(bold_font)
        
        self.category_listbox = QListWidget()
        self.category_listbox.currentRowChanged.connect(self.category_selection_changed)
        self.category_listbox.setItemDelegate(PaddingDelegate(v_padding=4))
        
        category_layout.addWidget(self.category_dropdown)
        category_layout.addWidget(self.category_listbox)
        
        self.main_splitter.addWidget(wrap_layout(category_layout))

        # subject selection panel
        self.subject_listbox = QListWidget()
        self.subject_listbox.itemSelectionChanged.connect(self.subject_selection_changed)
        self.subject_listbox.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.subject_listbox.setItemDelegate(PaddingDelegate(v_padding=4))
        
        subj_layout.addWidget(QLabel('<b>Subjects</b>'))
        subj_layout.addWidget(self.subject_listbox)
        
        self.main_splitter.addWidget(wrap_layout(subj_layout))

        # session table panel
        self.session_table = QTableWidget()
        self.session_table.itemSelectionChanged.connect(self.sess_selection_changed)
        self.session_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.session_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.session_table.verticalHeader().setVisible(False)
        #self.session_table.verticalHeader().setDefaultSectionSize(20)
        self.session_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.session_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.session_table.setItemDelegate(PaddingDelegate(h_padding=15))
        
        sess_layout.addWidget(QLabel('<b>Sessions</b>'))
        sess_layout.addWidget(self.session_table)
        
        self.main_splitter.addWidget(wrap_layout(sess_layout))

        # selected rat/session details
        subj_details_container = QGroupBox()
        self.subj_details_layout = QFormLayout()
        self.subj_details_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self.subj_details_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        self.subj_details_layout.setVerticalSpacing(12)
        self.subj_details_layout.setHorizontalSpacing(15)
        subj_details_container.setLayout(self.subj_details_layout)
        
        subj_label_box_layout = QVBoxLayout()
        subj_label_box_layout.setContentsMargins(0, 0, 0, 0)
        subj_label_box_layout.addWidget(QLabel('<b>Subject Info</b>'))
        subj_label_box_layout.addWidget(subj_details_container)
        
        session_details_container = QGroupBox()
        self.session_details_layout = QFormLayout()
        self.session_details_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self.session_details_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        self.session_details_layout.setVerticalSpacing(12)
        self.session_details_layout.setHorizontalSpacing(15)
        session_details_container.setLayout(self.session_details_layout)
        
        scroll = QScrollArea()
        scroll.setWidget(session_details_container)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        
        sess_label_box_layout = QVBoxLayout()
        sess_label_box_layout.setContentsMargins(0, 0, 0, 0)
        sess_label_box_layout.addWidget(QLabel('<b>Session Data</b>'))
        sess_label_box_layout.addWidget(scroll)

        settings_layout.addWidget(wrap_layout(subj_label_box_layout))
        settings_layout.addWidget(wrap_layout(sess_label_box_layout))
        
        self.main_splitter.addWidget(wrap_layout(settings_layout))
        
        # make the panels not collapsible
        for i in range(self.main_splitter.count()):
            self.main_splitter.setCollapsible(i, False)
        
        # select the first item in every panel by selecting the first category
        self.category_dropdown.setCurrentIndex(0)
        self.filter_category_changed()
        
        self.main_splitter.setSizes([200, 100, 1100, 500])
        
        
    def closeEvent(self, event):
        self.settings.setValue('geometry', self.saveGeometry())
        self.settings.setValue('windowState', self.saveState())
        self.settings.setValue('splitter', self.main_splitter.saveState())
        super().closeEvent(event)
        
        
    def restore_state(self):
        self.restoreGeometry(self.settings.value('geometry', b""))
        self.restoreState(self.settings.value('windowState', b""))
        self.main_splitter.restoreState(self.settings.value('splitter', b""))
        
        
    def filter_category_changed(self):
        self.selected_filter = self.category_dropdown.currentText()
        
        with handle_db_errors(self) as success:
            self.populate_filter_categories()
        
        if not success():
            # clear all downstream controls
            self.category_listbox.clear()
            self.selected_category = None
            self.selected_subjs = []
            self.selected_sess = None
            
            self.populate_subject_ids()
            self.populate_session_data()
            self.update_session_details()
        

    def category_selection_changed(self, index):
        if index < 0:
            self.selected_category = None
        else:
            sel_item = self.category_listbox.item(index)
            if sel_item:
                self.selected_category = sel_item.data(Qt.ItemDataRole.UserRole)
            else:
                self.selected_category = None
        
        with handle_db_errors(self) as success:
            self.populate_subject_ids()
        
        if not success():
            # clear all downstream controls
            self.subject_listbox.clear()
            self.selected_subjs = []
            self.selected_sess = None
            
            self.populate_session_data()
            self.update_session_details()


    def subject_selection_changed(self):
        selected_items = self.subject_listbox.selectedItems()
        
        if len(selected_items) == 0:
            self.selected_subjs = []
        else:
            self.selected_subjs = [item.text() for item in selected_items]
        
        with handle_db_errors(self) as success:
            self.populate_session_data()
            
        if not success():
            # clear all downstream controls
            self.session_table.clear()
            self.selected_sess = None

            self.update_session_details()


    def sess_selection_changed(self):
        selected_item = self.session_table.selectedItems()
        
        if not selected_item:
            self.selected_sess = None
        else:
            sessid = int(self.session_table.item(selected_item[0].row(), 1).text())
            self.selected_sess = self.sess_data[self.sess_data['sessid'] == sessid].iloc[0]
            
        self.update_session_details()
        

    def update_session_details(self):
        while self.subj_details_layout.count():
            item = self.subj_details_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(None)
        
        while self.session_details_layout.count():
            item = self.session_details_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(None)

        if not self.selected_sess is None:
            self.add_form_row(self.subj_details_layout, 'Id', str(self.selected_sess['subjid']))
            # calculate age
            years, days = difference_in_years_and_days(self.selected_sess['datearrived'], date.today())
            self.add_form_row(self.subj_details_layout, 'Age', '{} years, {} days'.format(years, days))
            self.add_form_row(self.subj_details_layout, 'Weight', str(self.selected_sess['mass']))
            
            sess_details = self.selected_sess['session_data']
            for key, val in sess_details.items():
                self.add_form_row(self.session_details_layout, key, str(val))
            
            
    def add_form_row(self, layout, label, value):
        value_field = QLabel(str(value))
        value_field.setStyleSheet("""
            QLabel {
                background-color: white;
                padding: 5px;
            }
        """)
        layout.addRow(QLabel(label), value_field)
    
    # Database Query Methods
    
    def populate_filter_categories(self):
        db = db_access._get_connector()
        cur = db.cursor(buffered=True)

        match self.selected_filter:
            case 'Protocol':
                query = 'select distinct protocol, protocol from beh.sessions where subjid != 0'
                
            case 'Rig':
                query = 'select rigid, rigid from met.rigs where rigid != 0'
                
            case 'Experimenter':
                query = ('select distinct e.firstname, e.experid from met.experimenters e '+
                         'join met.animals a on e.experid = a.experid where a.status != \'dead\'')
                
        cur.execute(query)
        vals = cur.fetchall()
        
        cur.close()
        db.close()

        # Sort list of tuples 
        self.category_list = sorted(vals, key=lambda tup: tup[0])
        
        # populate listbox
        self.category_listbox.clear()

        for item in self.category_list:
            list_item = QListWidgetItem(str(item[0]))
            list_item.setData(Qt.ItemDataRole.UserRole, item[1])
            self.category_listbox.addItem(list_item)
            
        if len(self.category_list) > 0:
            self.category_listbox.setCurrentRow(0)


    def populate_subject_ids(self):
        
        if not self.selected_category is None:
            db = db_access._get_connector()
            cur = db.cursor(buffered=True)

            match self.selected_filter:
                case 'Protocol':
                    query = ('select distinct a.subjid, a.status, max(s.sessiondate) from met.animals a join beh.sessions s '+
                             'on a.subjid = s.subjid where s.protocol = \'{}\' and a.subjid != 0 group by a.subjid'.format(self.selected_category))
                    
                case 'Rig':
                    query = ('select distinct a.subjid, a.status, max(s.sessiondate) from met.animals a join beh.sessions s '+
                             'on a.subjid = s.subjid where s.rigid = {} and a.subjid != 0 group by a.subjid'.format(self.selected_category))
                    
                case 'Experimenter':
                    query = ('select distinct a.subjid, a.status, max(s.sessiondate) from met.animals a join beh.sessions s on a.subjid = s.subjid '+
                             'join met.experimenters e on e.experid = a.experid where e.experid = {} and a.subjid != 0 group by a.subjid'.format(self.selected_category))
                
            cur.execute(query)
            vals = cur.fetchall()
            
            cur.close()
            db.close()
    
            # Sort list of tuples 
            self.subj_list = sorted(vals, key=lambda tup: (tup[1] == 'dead', -tup[2].toordinal(), tup[0]))
        
        else:
            self.subj_list = []
        
        # populate listbox
        self.subject_listbox.blockSignals(True)
        self.subject_listbox.clear()
        
        for item in self.subj_list:
            list_item = QListWidgetItem(str(item[0]))
            # if animal is dead, make background slightly grayer
            if item[1] == 'dead':
                list_item.setBackground(QColor(10,10,10,10))
                
            self.subject_listbox.addItem(list_item)
        
        self.subject_listbox.blockSignals(False)
        
        if len(self.subj_list) > 0:
            self.subject_listbox.setCurrentRow(0)
            
            
    def populate_session_data(self):
        
        column_labels = ['Subject', 'Session', 'Date', 'Protocol', 'Stage', '# Trials', 'Reward (μL)', 'Hits (%)', 'Viols (%)', 'Rig']
        
        if len(self.selected_subjs) > 0:
            db = db_access._get_connector()
            cur = db.cursor(dictionary=True, buffered=True)
            
            subj_str = ','.join(self.selected_subjs)

            data_query = ('select a.subjid, a.sessid, a.sessiondate, a.protocol, a.stage, a.num_trials, a.total_profit, '+
                     'a.hits, a.viols, a.rigid, b.settingsname, d.datearrived, e.session_data '+
                     'from beh.sessview a '+
                     'join beh.sessions s on a.sessid = s.sessid '+
                     'join met.settings b on s.expgroupid = b.expgroupid and s.startstage = b.stage '+
                     'join met.animals d on a.subjid = d.subjid '+
                     'join beh.sess_ended_data e on a.sessid = e.sessid '
                     'where a.subjid in ({})').format(subj_str)
            
            mass_query = 'select subjid, mass, mdate from met.mass where subjid in ({})'.format(subj_str)
            
            #start = time.perf_counter()
            cur.execute(data_query)
            data_df = pd.DataFrame(cur.fetchall())
            #print('Retrieved behavioral data for subjects {} in {:.2f} s'.format(subj_str, time.perf_counter()-start))
            
            #start = time.perf_counter()
            cur.execute(mass_query)
            mass_df = pd.DataFrame(cur.fetchall())
            #print('Retrieved mass data for subjects {} in {:.2f} s'.format(subj_str, time.perf_counter()-start))
            
            # convert to dates for matching
            #start = time.perf_counter()
            data_df['sessiondate'] = pd.to_datetime(data_df['sessiondate']).dt.date
            mass_df['mdate'] = pd.to_datetime(mass_df['mdate']).dt.date
            
            merged_df = pd.merge(data_df, mass_df, how='left', left_on=['subjid', 'sessiondate'], right_on=['subjid', 'mdate']).drop(columns=['mdate'])
            #print('Merged tables in {:.2f} s'.format(time.perf_counter()-start))

            cur.close()
            db.close()
    
            # convert to dataframe
            self.sess_data = merged_df.sort_values(by='sessid', ascending=False).reset_index(drop=True)
            # add stage name to number
            self.sess_data['stage'] = self.sess_data['stage'].apply(lambda x: int(x) if x == int(x) else x).astype(str) + '-' + self.sess_data['settingsname']
            # parse session data to json
            self.sess_data['session_data'] = self.sess_data['session_data'].apply(db_access._parse_json)
        else:
            self.sess_data = pd.DataFrame()
        
        # populate table
        self.session_table.blockSignals(True)
        self.session_table.clear()
        self.session_table.setRowCount(len(self.sess_data))
        self.session_table.setColumnCount(len(column_labels))
        self.session_table.setHorizontalHeaderLabels(column_labels)

        for i, row in self.sess_data.iterrows():
            for j, col in enumerate(column_labels):
                item = QTableWidgetItem(str(row.iloc[j]))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                item.setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                self.session_table.setItem(i, j, item)

                
        self.session_table.blockSignals(False)
        
        if len(self.sess_data) > 0:
            self.session_table.selectRow(0)
        
# %% Main

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BehaviorTracker()
    window.show()
    sys.exit(app.exec())
