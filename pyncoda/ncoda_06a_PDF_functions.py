import csv
from fpdf import FPDF, TitleStyle


"""
Help to make Codebook PDF
https://pyfpdf.github.io/fpdf2/Tutorial.html#tuto-5-creating-tables
https://github.com/bvalgard/create-pdf-with-python-fpdf2

# Code for fpdf
https://github.com/PyFPDF/fpdf2/blob/master/fpdf/fpdf.py
# Docs for fpdf
https://pyfpdf.github.io/fpdf2/index.html

RGB color codes
https://www.rapidtables.com/web/color/RGB_Color.html

Possible alternative to fpdf
https://github.com/jorisschellekens/borb

"""

# Header is a FPDF2 function that is called with addpage
class PDF(FPDF):
    def __init__(self,
            header_text: str = "Header Text",
            footer_text: str = "Footer Text",
            image_path:  str = ""):
            
        # Help with understating super().__init__()
        # https://rhettinger.wordpress.com/2011/05/26/super-considered-super/
        # Super is a function that calls the parent class
        # Need to set the page format here
        super().__init__(orientation = "P", unit = "mm", format = "letter")
        self.header_text = header_text
        self.footer_text = footer_text
        self.image_path = image_path
        
    def header(self):
        # Setting font: helvetica bold 15
        self.set_font("helvetica", "B", 15)
        # Moving cursor to the right:
        self.cell(w = 80)
        #pdf.set_y(-10)
        # Printing title:
        self.cell(w = 30, 
                  h = 10, 
                  txt = self.header_text, 
                  border = 0, ln = 0, align = "C")
        # Performing a line break:
        self.ln(15)

    # Footer is a FPDF2 function that is called with addpage
    def footer(self):
        # Rendering logo:
        if self.image_path != "":
            self.image(name = self.image_path, w=self.epw, x = 15, y = self.eph+10)
        # Position cursor at 1.4 inches from bottom:
        self.set_y(self.eph-10)
        # Setting font: helvetica italic 8
        self.set_font("helvetica", "I", 8)
        # Printing page number:
        self.ln(23)
        self.cell(w = 0, h = 10, 
                    txt = f"Page {self.page_no()}/{{nb}}",
                    border = 0, ln = 0, align = "C")
        self.ln()
        self.cell(w = 0, h = 0, txt = self.footer_text, 
                    border = 0, ln = 0, align = "C")

    ## TABLE FUNCTIONS
    # Code from: https://github.com/bvalgard/create-pdf-with-python-fpdf2/blob/main/table_function.py

    def get_col_widths(self, cell_width, data, table_data):
        """
        function to Get Width of Columns for tables
        """
        col_width = cell_width
        if col_width == 'even':
            col_width = self.epw / len(data[0]) - 1  
            # distribute content evenly   
            # epw = effective page width (width of page not including margins)
        elif col_width == 'uneven':
            col_widths = []
            # searching through columns for largest sized cell (not rows but cols)
            for col in range(len(table_data[0])): # for every row
                longest = 0 
                for row in range(len(table_data)):
                    cell_value = str(table_data[row][col])
                    value_length = self.get_string_width(cell_value)
                    if value_length > longest:
                        longest = value_length
                col_widths.append(longest + 4) # add 4 for padding
            col_width = col_widths

        # Add new option for a 20% 80% split
        elif col_width == 'split-20-80':
            col_80width = int(self.epw * 0.8)
            col_20width = int(self.epw - col_80width)
            col_width = [col_20width, col_80width]

        return col_width

    def create_table(self,
                    table_data, 
                    title='', 
                    data_size = 10, 
                    title_size=12, 
                    align_data='L', 
                    align_header='L', 
                    cell_width='uneven',
                    line_space = 2.5):
        """
        Code from: https://github.com/bvalgard/create-pdf-with-python-fpdf2/blob/main/table_function.py
        table_data: 
                    list of lists with first element being list of headers
        title: 
                    (Optional) title of table (optional)
        data_size: 
                    the font size of table data
        title_size: 
                    the font size fo the title of the table
        align_data: 
                    align table data
                    L = left align
                    C = center align
                    R = right align
        align_header: 
                    align table data
                    L = left align
                    C = center align
                    R = right align
        cell_width: 
                    even: evenly distribute cell/column width
                    uneven: base cell size on length of cell/column items
                    int: int value for width of each cell/column
                    list of ints: list equal to number of columns with the width of each cell / column

        line_space: 
                    Spacing between rows in table
        """

        self.set_font("helvetica", size=title_size)
        line_height = self.font_size * line_space

        # Set table data and headers
        header = table_data[0]
        data = table_data[1:]

        # Get column widths
        col_width = self.get_col_widths(
                                cell_width=cell_width, 
                                data=data, 
                                table_data=table_data)

        # TABLE CREATION #
        # add title
        print(title)
        if title != '':
            self.multi_cell(0, line_height, title, 
                    border=0, align='j')
            self.ln(line_height) # move cursor back to the left margin
        
        self.set_font("helvetica",size=data_size)
        # add header
        y1 = self.get_y()
        x_left = self.get_x()
        x_right = self.epw + x_left

        # Add header row
        for i in range(len(header)):
            datum = header[i]
            self.multi_cell(col_width[i], line_height, 
                    datum, border=0, 
                    align=align_header, ln=3, 
                    max_line_height=self.font_size)
            x_right = self.get_x()
        self.ln(line_height) # move cursor back to the left margin
        y2 = self.get_y()
        # Add lines around headers
        self.line(x_left,y1,x_right,y1)
        self.line(x_left,y2,x_right,y2)

        # Add Data
        self.set_fill_color(224, 235, 255)
        fill = False
        # loop over rows
        for i in range(len(data)):
            row = data[i]
            # Loop over columns
            for i in range(len(row)):
                datum = row[i]
                if not isinstance(datum, str):
                    datum = str(datum)
                adjusted_col_width = col_width[i]
                self.multi_cell(adjusted_col_width, 
                        line_height, datum, 
                        border=0, align=align_data, ln=3,
                        max_line_height=self.font_size* line_space,
                        fill = fill) 
            fill = not fill
            self.ln(self.font_size * line_space) # move cursor back to the left margin
        # Add line to bottom of table
        y3 = self.get_y()+1
        self.line(x_left,y3,x_right,y3)