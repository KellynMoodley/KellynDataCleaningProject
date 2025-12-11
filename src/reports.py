"""
Reports Module
Generates PDF and CSV reports for included and excluded data.
"""

import csv
import io
from typing import List, Dict, Any
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates various reports for cleaned data"""
    
    def __init__(self):
        """Initialize report generator"""
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
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
    
    def generate_csv(self, data: List[Dict[str, Any]], columns: List[str]) -> str:
        """
        Generate CSV string from data
        
        Args:
            data: List of dictionaries
            columns: List of column names to include
        
        Returns:
            CSV string
        """
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()
    
    def generate_included_pdf(self, data: List[Dict[str, Any]], 
                              analytics: Dict[str, Any],
                              sheet_name: str) -> bytes:
        """
        Generate PDF report for included data
        
        Args:
            data: List of included records
            analytics: Analytics dictionary
            sheet_name: Name of the sheet
        
        Returns:
            PDF bytes
        """
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        story = []
        
        # Title
        title = Paragraph(f"Data Included Report - {sheet_name}", self.styles['CustomTitle'])
        story.append(title)
        
        # Subtitle with date
        subtitle = Paragraph(
            f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
            self.styles['CustomSubtitle']
        )
        story.append(subtitle)
        story.append(Spacer(1, 0.3*inch))
        
        # Summary section
        story.append(Paragraph("Summary Statistics", self.styles['SectionHeader']))
        
        dataset_sizes = analytics.get('dataset_sizes', {})
        uniqueness = analytics.get('uniqueness_metrics', {})
        
        summary_data = [
            ['Metric', 'Value'],
            ['Total Original Rows', f"{dataset_sizes.get('original_row_count', 0):,}"],
            ['Included Rows', f"{dataset_sizes.get('included_row_count', 0):,}"],
            ['Excluded Rows', f"{dataset_sizes.get('excluded_row_count', 0):,}"],
            ['Inclusion Rate', f"{dataset_sizes.get('percent_included_vs_original', 0):.2f}%"],
            ['', ''],
            ['Unique Names', f"{uniqueness.get('unique_names', 0):,}"],
            ['Unique Birthday Combinations', f"{uniqueness.get('unique_birthday_combinations', 0):,}"],
            ['Unique Name+Year Combinations', f"{uniqueness.get('unique_name_year', 0):,}"],
        ]
        
        summary_table = Table(summary_data, colWidths=[3.5*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 0.4*inch))
        
        # Data section
        story.append(PageBreak())
        story.append(Paragraph("Included Data Records", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        # Show first 100 records in PDF
        display_data = data[:100] if len(data) > 100 else data
        
        if display_data:
            # Prepare table data with proper UUID display
            table_data = [['Row ID', 'Name', 'Day', 'Month', 'Year']]
            
            for row in display_data:
                # Format UUID to show full value
                row_id = str(row.get('row_id', ''))
                table_data.append([
                    Paragraph(row_id, self.styles['Normal']),  # Use Paragraph for UUID wrapping
                    row.get('name', ''),
                    str(row.get('birth_day', '')),
                    str(row.get('birth_month', '')),
                    str(row.get('birth_year', ''))
                ])
            
            # Adjust column widths to accommodate full UUIDs
            data_table = Table(table_data, colWidths=[2.2*inch, 2*inch, 0.6*inch, 0.7*inch, 0.7*inch])
            data_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2ECC71')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('TOPPADDING', (0, 1), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            story.append(data_table)
            
            if len(data) > 100:
                story.append(Spacer(1, 0.2*inch))
                note = Paragraph(
                    f"<i>Note: Showing first 100 records of {len(data):,} total records. "
                    f"Download CSV for complete data.</i>",
                    self.styles['Normal']
                )
                story.append(note)
        
        # Build PDF
        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes
    
    def generate_excluded_pdf(self, data: List[Dict[str, Any]], 
                              analytics: Dict[str, Any],
                              sheet_name: str) -> bytes:
        """
        Generate PDF report for excluded data
        
        Args:
            data: List of excluded records
            analytics: Analytics dictionary
            sheet_name: Name of the sheet
        
        Returns:
            PDF bytes
        """
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        story = []
        
        # Title
        title = Paragraph(f"Data Exclusion Report - {sheet_name}", self.styles['CustomTitle'])
        story.append(title)
        
        # Subtitle
        subtitle = Paragraph(
            f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
            self.styles['CustomSubtitle']
        )
        story.append(subtitle)
        story.append(Spacer(1, 0.3*inch))
        
        # Summary section
        story.append(Paragraph("Exclusion Summary", self.styles['SectionHeader']))
        
        dataset_sizes = analytics.get('dataset_sizes', {})
        exclusion_reasons = analytics.get('exclusion_reasons', [])
        
        summary_data = [
            ['Metric', 'Value'],
            ['Total Excluded Rows', f"{dataset_sizes.get('excluded_row_count', 0):,}"],
            ['Exclusion Rate', f"{dataset_sizes.get('percent_excluded_vs_original', 0):.2f}%"],
        ]
        
        summary_table = Table(summary_data, colWidths=[3.5*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 0.3*inch))
        
        # Exclusion reasons breakdown
        if exclusion_reasons:
            story.append(Paragraph("Exclusion Reasons Breakdown", self.styles['SectionHeader']))
            
            reason_data = [['Exclusion Reason', 'Count']]
            for reason_item in exclusion_reasons[:15]:  # Top 15 reasons
                reason_data.append([
                    reason_item.get('reason', ''),
                    f"{reason_item.get('count', 0):,}"
                ])
            
            reason_table = Table(reason_data, colWidths=[4*inch, 1.5*inch])
            reason_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E67E22')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('TOPPADDING', (0, 1), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 5),
            ]))
            story.append(reason_table)
            story.append(Spacer(1, 0.4*inch))
        
        # Excluded records
        story.append(PageBreak())
        story.append(Paragraph("Excluded Records", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        # Show first 100 records
        display_data = data[:100] if len(data) > 100 else data
        
        if display_data:
            table_data = [['Row ID', 'Name', 'Day', 'Month', 'Year', 'Reason']]
            
            for row in display_data:
                # Truncate reason if too long for display
                reason = row.get('exclusion_reason', '')
                if len(reason) > 40:
                    reason = reason[:37] + '...'
                
                row_id = str(row.get('row_id', ''))[:8]  # Show first 8 chars of UUID
                
                table_data.append([
                    row_id,
                    row.get('original_name', '-') or '-',
                    row.get('original_birth_day', '-') or '-',
                    row.get('original_birth_month', '-') or '-',
                    row.get('original_birth_year', '-') or '-',
                    reason
                ])
            
            data_table = Table(table_data, colWidths=[0.8*inch, 1.2*inch, 0.5*inch, 0.6*inch, 0.6*inch, 2.5*inch])
            data_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#C0392B')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
                ('TOPPADDING', (0, 1), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
            ]))
            story.append(data_table)
            
            if len(data) > 100:
                story.append(Spacer(1, 0.2*inch))
                note = Paragraph(
                    f"<i>Note: Showing first 100 records of {len(data):,} total excluded records. "
                    f"Download CSV for complete data.</i>",
                    self.styles['Normal']
                )
                story.append(note)
        
        # Build PDF
        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes