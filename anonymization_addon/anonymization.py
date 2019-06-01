# -*- encoding: utf-8 -*-
from lxml import etree
import os
import base64
try:
    import cPickle as pickle
except ImportError:
    import pickle
import random
import datetime
from openerp.osv import fields, osv
from openerp.tools.translate import _
from openerp.tools.safe_eval import safe_eval as eval

from itertools import groupby


class ir_model_fields_anonymize_wizard(osv.osv_memory):
    _inherit = 'ir.model.fields.anonymize.wizard'


    def anonymize_database(self, cr, uid, ids, context=None):
        """Sets the 'anonymized' state to defined fields"""

        # create a new history record:
        anonymization_history_model = self.pool.get('ir.model.fields.anonymization.history')

        vals = {
            'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'state': 'started',
            'direction': 'clear -> anonymized',
        }
        history_id = anonymization_history_model.create(cr, uid, vals)

        # check that all the defined fields are in the 'clear' state
        state = self.pool.get('ir.model.fields.anonymization')._get_global_state(cr, uid, context=context)
        if state == 'anonymized':
            self._raise_after_history_update(cr, uid, history_id, _('Error !'), _("The database is currently anonymized, you cannot anonymize it again."))
        elif state == 'unstable':
            msg = _("The database anonymization is currently in an unstable state. Some fields are anonymized," + \
                  " while some fields are not anonymized. You should try to solve this problem before trying to do anything.")
            self._raise_after_history_update(cr, uid, history_id, 'Error !', msg)

        # do the anonymization:
        dirpath = os.environ.get('HOME') or os.getcwd()
        rel_filepath = 'field_anonymization_%s_%s.pickle' % (cr.dbname, history_id)
        abs_filepath = os.path.abspath(os.path.join(dirpath, rel_filepath))

        ir_model_fields_anonymization_model = self.pool.get('ir.model.fields.anonymization')
        field_ids = ir_model_fields_anonymization_model.search(cr, uid, [('state', '<>', 'not_existing')], context=context)
        fields = ir_model_fields_anonymization_model.browse(cr, uid, field_ids, context=context)

        if not fields:
            msg = "No fields are going to be anonymized."
            self._raise_after_history_update(cr, uid, history_id, 'Error !', msg)

        data = []

        for field in fields:
            model_name = field.model_id.model
            field_name = field.field_id.name
            field_type = field.field_id.ttype
            table_name = self.pool.get(model_name)._table

            # get the current value
            sql = "select id, %s from %s" % (field_name, table_name)
            cr.execute(sql)
            records = cr.dictfetchall()
            for record in records:
                data.append({"model_id": model_name, "field_id": field_name, "id": record['id'], "value": record[field_name]})

                # anonymize the value:
                anonymized_value = None

                sid = str(record['id'])
                if field_type == 'char':
                    anonymized_value = 'xxx'+sid
                elif field_type == 'selection':
                    anonymized_value = 'xxx'+sid
                elif field_type == 'text':
                    anonymized_value = 'xxx'+sid
                elif field_type == 'boolean':
                    anonymized_value = random.choice([True, False])
                elif field_type == 'date':
                    anonymized_value = '2011-11-11'
                elif field_type == 'datetime':
                    anonymized_value = '2011-11-11 11:11:11'
                elif field_type == 'float':
                    anonymized_value = 0.0
                elif field_type == 'integer':
                    anonymized_value = 0
                elif field_type in ['binary', 'many2many', 'many2one', 'one2many', 'reference']: # cannot anonymize these kind of fields
                    msg = _("Cannot anonymize fields of these types: binary, many2many, many2one, one2many, reference.")
                    self._raise_after_history_update(cr, uid, history_id, 'Error !', msg)

                if anonymized_value is None:
                    self._raise_after_history_update(cr, uid, history_id, _('Error !'), _("Anonymized value is None. This cannot happens."))

                sql = "update %(table)s set %(field)s = %%(anonymized_value)s where id = %%(id)s" % {
                    'table': table_name,
                    'field': field_name,
                }
                cr.execute(sql, {
                    'anonymized_value': anonymized_value,
                    'id': record['id']
                })

        # save pickle:
        fn = open(abs_filepath, 'w')
        pickle.dump(data, fn, pickle.HIGHEST_PROTOCOL)

        # update the anonymization fields:
        values = {
            'state': 'anonymized',
        }
        ir_model_fields_anonymization_model.write(cr, uid, field_ids, values, context=context)

        # add a result message in the wizard:
        msgs = ["Anonymization successful.",
               "",
               "Donot forget to save the resulting file to a safe place because you will not be able to revert the anonymization without this file.",
               "",
               "This file is also stored in the %s directory. The absolute file path is: %s.",
              ]
        msg = '\n'.join(msgs) % (dirpath, abs_filepath)

        fn = open(abs_filepath, 'r')

        self.write(cr, uid, ids, {
            'msg': msg,
            # 'file_export': base64.encodestring(fn.read()),
        })
        fn.close()

        # update the history record:
        anonymization_history_model.write(cr, uid, history_id, {
            'field_ids': [[6, 0, field_ids]],
            'msg': msg,
            'filepath': abs_filepath,
            'state': 'done',
        })

        # handle the view:
        view_id = self._id_get(cr, uid, 'ir.ui.view', 'view_ir_model_fields_anonymize_wizard_form', 'anonymization')

        return {
                'res_id': ids[0],
                'view_id': [view_id],
                'view_type': 'form',
                "view_mode": 'form',
                'res_model': 'ir.model.fields.anonymize.wizard',
                'type': 'ir.actions.act_window',
                'context': {'step': 'just_anonymized'},
                'target': 'new',
        }
