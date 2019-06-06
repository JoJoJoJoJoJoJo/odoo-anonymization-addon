# -*- coding: utf-8 -*-
import base64
import os

from lxml import etree
from operator import itemgetter

from odoo import api, fields, models, _
from odoo.exceptions import UserError
from odoo.release import version_info
from odoo.tools import pickle
from odoo.tools.safe_eval import safe_eval
from odoo.addons.anonymization.models.anonymization import group

import logging
_logger = logging.getLogger(__name__)


class IrModelFieldsAnonymizeWizard(models.TransientModel):
    _inherit = 'ir.model.fields.anonymize.wizard'

    file_import_path = fields.Char(
        'Import Path',
        help="This is the file path created by the anonymization process. It should have the '.pickle' extention."
    )

    @api.multi
    def reverse_anonymize_database(self):
        """Set the 'clear' state to defined fields"""
        self.ensure_one()
        IrModelFieldsAnonymization = self.env['ir.model.fields.anonymization']

        # check that all the defined fields are in the 'anonymized' state
        state = IrModelFieldsAnonymization._get_global_state()
        if state == 'clear':
            raise UserError(_("The database is not currently anonymized, you cannot reverse the anonymization."))
        elif state == 'unstable':
            raise UserError(_("The database anonymization is currently in an unstable state. Some fields are anonymized,"
                              " while some fields are not anonymized. You should try to solve this problem before trying to do anything."))

        # if not self.file_import:
        #     raise UserError('%s: %s' % (_('Error !'), _("It is not possible to reverse the anonymization process without supplying the anonymization export file.")))

        # reverse the anonymization:
        # load the pickle file content into a data structure:
        if self.file_import:
            _logger.info('Getting pickle from the upload file')
            data = pickle.loads(base64.decodestring(self.file_import))
        else:
            _logger.info('Getting pickle from the path %s', self.file_import_path)
            data = pickle.loads(open(self.file_import_path, 'rb').read())
        fixes = self.env['ir.model.fields.anonymization.migration.fix'].search_read([
            ('target_version', '=', '.'.join(map(str, version_info[:2])))
        ], ['model_name', 'field_name', 'query', 'query_type', 'sequence'])
        fixes = group(fixes, ('model_name', 'field_name'))
        _logger.info('Ready to do the unanonymization.')
        i = 0
        for line in data:
            i += 1
            _logger.info('Running unanonymization process: %s/%s', i, len(data))
            queries = []
            table_name = self.env[line['model_id']]._table if line['model_id'] in self.env else None

            # check if custom sql exists:
            key = (line['model_id'], line['field_id'])
            custom_updates = fixes.get(key)
            if custom_updates:
                custom_updates.sort(key=itemgetter('sequence'))
                queries = [(record['query'], record['query_type']) for record in custom_updates if record['query_type']]
            elif table_name:
                queries = [('update "%(table)s" set "%(field)s" = %%(value)s where id = %%(id)s' % {
                    'table': table_name,
                    'field': line['field_id'],
                }, 'sql')]

            for query in queries:
                if query[1] == 'sql':
                    self.env.cr.execute(query[0], {
                        'value': line['value'],
                        'id': line['id']
                    })
                elif query[1] == 'python':
                    safe_eval(query[0] % line)
                else:
                    raise Exception("Unknown query type '%s'. Valid types are: sql, python." % (query['query_type'], ))
        _logger.info('Unanonymization done.')
        # update the anonymization fields:
        ano_fields = IrModelFieldsAnonymization.search([('state', '!=', 'not_existing')])
        ano_fields.write({'state': 'clear'})

        # add a result message in the wizard:
        self.msg = '\n'.join(["Successfully reversed the anonymization.", ""])

        # create a new history record:
        history = self.env['ir.model.fields.anonymization.history'].create({
            'date': fields.Datetime.now(),
            'field_ids': [[6, 0, ano_fields.ids]],
            'msg': self.msg,
            'filepath': self.file_import_path,
            'direction': 'anonymized -> clear',
            'state': 'done'
        })

        return {
            'res_id': self.id,
            'view_id': self.env.ref('anonymization.view_ir_model_fields_anonymize_wizard_form').ids,
            'view_type': 'form',
            "view_mode": 'form',
            'res_model': 'ir.model.fields.anonymize.wizard',
            'type': 'ir.actions.act_window',
            'context': {'step': 'just_desanonymized'},
            'target': 'new'
        }

    @api.model
    def fields_view_get(self, view_id=None, view_type='form', toolbar=False, submenu=False):
        state = self.env['ir.model.fields.anonymization']._get_global_state()
        step = self.env.context.get('step', 'new_window')
        res = super(IrModelFieldsAnonymizeWizard, self).fields_view_get(view_id=view_id, view_type=view_type, toolbar=toolbar, submenu=submenu)
        eview = etree.fromstring(res['arch'])
        placeholder = eview.xpath("group[@name='placeholder1']")
        if len(placeholder):
            placeholder = placeholder[0]
            if step == 'new_window' and state == 'clear':
                # clicked in the menu and the fields are not anonymized: warn the admin that backuping the db is very important
                placeholder.addnext(etree.Element('field', {'name': 'msg', 'colspan': '4', 'nolabel': '1'}))
                placeholder.addnext(etree.Element('newline'))
                placeholder.addnext(etree.Element('label', {'string': 'Warning'}))
                eview.remove(placeholder)
            elif step == 'new_window' and state == 'anonymized':
                # clicked in the menu and the fields are already anonymized
                placeholder.addnext(etree.Element('newline'))
                placeholder.addnext(etree.Element('field', {'name': 'file_import'}))
                placeholder.addnext(etree.Element('label', {'string': 'Anonymization file'}))
                placeholder.addnext(etree.Element('newline'))
                placeholder.addnext(etree.Element('field', {'name': 'file_import_path'}))
                placeholder.addnext(etree.Element('label', {'string': 'Pickle Path'}))
                eview.remove(placeholder)
            elif step == 'just_anonymized':
                # we just ran the anonymization process, we need the file export field
                placeholder.addnext(etree.Element('newline'))
                placeholder.addnext(etree.Element('field', {'name': 'file_export'}))
                # we need to remove the button:
                buttons = eview.xpath("button")
                for button in buttons:
                    eview.remove(button)
                # and add a message:
                placeholder.addnext(etree.Element('field', {'name': 'msg', 'colspan': '4', 'nolabel': '1'}))
                placeholder.addnext(etree.Element('newline'))
                placeholder.addnext(etree.Element('label', {'string': 'Result'}))
                # remove the placeholer:
                eview.remove(placeholder)
            elif step == 'just_desanonymized':
                # we just reversed the anonymization process, we don't need any field
                # we need to remove the button
                buttons = eview.xpath("button")
                for button in buttons:
                    eview.remove(button)
                # and add a message
                placeholder.addnext(etree.Element('field', {'name': 'msg', 'colspan': '4', 'nolabel': '1'}))
                placeholder.addnext(etree.Element('newline'))
                placeholder.addnext(etree.Element('label', {'string': 'Result'}))
                # remove the placeholer:
                eview.remove(placeholder)
            else:
                raise UserError(_("The database anonymization is currently in an unstable state. Some fields are anonymized,"
                                  " while some fields are not anonymized. You should try to solve this problem before trying to do anything else."))
            res['arch'] = etree.tostring(eview)
        return res
