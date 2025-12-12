from typing import List, Dict, Any
import io
import csv
import psycopg2
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER
from datetime import datetime

class ReportGenerator:
    """Generates PDF and CSV reports using direct PostgreSQL connection"""
    
    # Database connection parameters
    DB_CONFIG = {
        'user': "postgres.bzpeybznqqhdsgaapaus",
        'password': "H4nDg2k68vodElAY",
        'host': "aws-1-eu-west-1.pooler.supabase.com",
        'port': "5432",
        'dbname': "postgres",
        'sslmode': 'require'
    }
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        # Title style
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#2C3E50'),
            spaceAfter=30,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        # Subtitle style
        self.styles.add(ParagraphStyle(
            name='CustomSubtitle',
            parent=self.styles['Normal'],
            fontSize=12,
            textColor=colors.HexColor('#7F8C8D'),
            spaceAfter=20,
            alignment=TA_CENTER
        ))
        # Section header
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#34495E'),
            spaceAfter=12,
            spaceBefore=12,
            fontName='Helvetica-Bold'
        ))
    
    def _get_connection(self):
        """Create and return a database connection"""
        return psycopg2.connect(**self.DB_CONFIG)
    
    def _fetch_included_data(self, table_name: str, sheet_identifier: str) -> List[Dict[str, Any]]:
        """Fetch included data directly from PostgreSQL"""
        connection = None
        cursor = None
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Query for included data
            query = f"""
                SELECT row_id, name, birth_day, birth_month, birth_year
                FROM {table_name}
                WHERE sheet_identifier = %s AND is_excluded = FALSE
                ORDER BY row_id
            """
            
            cursor.execute(query, (sheet_identifier,))
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in rows:
                data.append({
                    'row_id': row[0],
                    'name': row[1],
                    'birth_day': row[2],
                    'birth_month': row[3],
                    'birth_year': row[4]
                })
            
            return data
            
        except Exception as e:
            print(f"Error fetching included data: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def _fetch_excluded_data(self, table_name: str, sheet_identifier: str) -> List[Dict[str, Any]]:
        """Fetch excluded data directly from PostgreSQL"""
        connection = None
        cursor = None
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Query for excluded data
            query = f"""
                SELECT row_id, original_name, original_birth_day, 
                       original_birth_month, original_birth_year, exclusion_reason
                FROM {table_name}
                WHERE sheet_identifier = %s AND is_excluded = TRUE
                ORDER BY row_id
            """
            
            cursor.execute(query, (sheet_identifier,))
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in rows:
                data.append({
                    'row_id': row[0],
                    'original_name': row[1],
                    'original_birth_day': row[2],
                    'original_birth_month': row[3],
                    'original_birth_year': row[4],
                    'exclusion_reason': row[5]
                })
            
            return data
            
        except Exception as e:
            print(f"Error fetching excluded data: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def generate_csv(self, data: List[Dict[str, Any]], columns: List[str]) -> str:
        """Generate CSV string from data"""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()
    
    def generate_csv_direct(self, table_name: str, sheet_identifier: str, 
                           is_excluded: bool) -> str:
        """Generate CSV directly from PostgreSQL using COPY command"""
        connection = None
        cursor = None
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Create temporary view for the specific sheet and exclusion status
            if is_excluded:
                query = f"""
                    COPY (
                        SELECT row_id, original_name, original_birth_day, 
                               original_birth_month, original_birth_year, exclusion_reason
                        FROM {table_name}
                        WHERE sheet_identifier = '{sheet_identifier}' AND is_excluded = TRUE
                        ORDER BY row_id
                    ) TO STDOUT WITH CSV HEADER
                """
            else:
                query = f"""
                    COPY (
                        SELECT row_id, name, birth_day, birth_month, birth_year
                        FROM {table_name}
                        WHERE sheet_identifier = '{sheet_identifier}' AND is_excluded = FALSE
                        ORDER BY row_id
                    ) TO STDOUT WITH CSV HEADER
                """
            
            # Use StringIO to capture CSV output
            output = io.StringIO()
            cursor.copy_expert(query, output)
            csv_data = output.getvalue()
            output.close()
            
            return csv_data
            
        except Exception as e:
            print(f"Error generating CSV: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def generate_included_pdf(self, data: List[Dict[str, Any]], sheet_name: str) -> bytes:
        """Generate PDF report for included data"""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        story = []

        # Title
        story.append(Paragraph(f"Included Data Report - {sheet_name}", self.styles['CustomTitle']))
        story.append(Paragraph(f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", self.styles['CustomSubtitle']))
        story.append(Spacer(1, 0.3*inch))

        # Data section
        story.append(Paragraph("Included Data Records", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))

        if data:
            table_data = [['Row ID', 'Name', 'Day', 'Month', 'Year']]
            for row in data:
                table_data.append([
                    str(row.get('row_id', '')),
                    row.get('name', ''),
                    str(row.get('birth_day', '')),
                    str(row.get('birth_month', '')),
                    str(row.get('birth_year', ''))
                ])
            
            data_table = Table(table_data, colWidths=[2*inch, 2*inch, 0.6*inch, 0.7*inch, 0.7*inch])
            data_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2ECC71')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ]))
            story.append(data_table)
        else:
            story.append(Paragraph("No included data records found.", self.styles['Normal']))

        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes
    
    def generate_included_pdf_from_db(self, table_name: str, sheet_identifier: str, 
                                     sheet_name: str) -> bytes:
        """Generate PDF report for included data by fetching from database"""
        data = self._fetch_included_data(table_name, sheet_identifier)
        return self.generate_included_pdf(data, sheet_name)

    def generate_excluded_pdf(self, data: List[Dict[str, Any]], sheet_name: str) -> bytes:
        """Generate PDF report for excluded data"""
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        story = []

        # Title
        story.append(Paragraph(f"Excluded Data Report - {sheet_name}", self.styles['CustomTitle']))
        story.append(Paragraph(f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", self.styles['CustomSubtitle']))
        story.append(Spacer(1, 0.3*inch))

        # Excluded data section
        story.append(Paragraph("Excluded Data Records", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))

        if data:
            table_data = [['Row ID', 'Name', 'Day', 'Month', 'Year', 'Reason']]
            for row in data:
                table_data.append([
                    str(row.get('row_id', '')),
                    row.get('original_name', '-') or '-',
                    str(row.get('original_birth_day', '-') or '-'),
                    str(row.get('original_birth_month', '-') or '-'),
                    str(row.get('original_birth_year', '-') or '-'),
                    row.get('exclusion_reason', '')
                ])
            
            data_table = Table(table_data, colWidths=[0.8*inch, 1.2*inch, 0.5*inch, 0.6*inch, 0.6*inch, 2.5*inch])
            data_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#C0392B')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ]))
            story.append(data_table)
        else:
            story.append(Paragraph("No excluded data records found.", self.styles['Normal']))

        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes
    
    def generate_excluded_pdf_from_db(self, table_name: str, sheet_identifier: str, 
                                     sheet_name: str) -> bytes:
        """Generate PDF report for excluded data by fetching from database"""
        data = self._fetch_excluded_data(table_name, sheet_identifier)
        return self.generate_excluded_pdf(data, sheet_name)