/*
 * jQuery File Upload Plugin JS Example 8.8.2
 * https://github.com/blueimp/jQuery-File-Upload
 *
 * Copyright 2010, Sebastian Tschan
 * https://blueimp.net
 *
 * Licensed under the MIT license:
 * http://www.opensource.org/licenses/MIT
 */

/*jslint nomen: true, regexp: true */
/*global $, window, blueimp */
'use strict';

let dominator = {

  init: function(DOMelement) {
    this.el = DOMelement;
    this.files = new Map();   // Metadata related to uploaded files
    this.queueFiles = 0;      // Files in the process queue
    this.queueReady = false;
    this.processedFiles = 0;  // Number of succesfully processed files
  },

  /**
   * Callback function triggered when files have been added to upload queue
   */
  add: (e, data) => {
    dominator.queueFiles++
    let element = $('select#' + data.files[0].name.split('.')[0]);
    if(dominator.queueFiles <= 2) {
      dominator.files.set(element[0].id,
        {
          type: ''
        }
      );
      element.on('change', (e) => {
        dominator.setType(element[0].id, e.currentTarget.value);
      });
    } else {
      console.log('aborta mision');
      data.abort(); // TODO make it abort add
    }
    if(dominator.queueFiles == 2) {
      // TODO I thougt I could re-enable buttons here but no
    }
  },

  beforeUpload: data => {
    if(dominator.queueFiles == 2) {
      let name = data.get('file').name.split('.')[0];
      let file_type = dominator.files.get(name).type;
      data.set('file_type', file_type);
      $('button.process').removeAttr('disabled');
    } else {
      alert('There must be at least 2 files to upload!');
    }
    return data;
  },

  /**
   * Callback function to send uploaded files to processing in sqlizer
   */
  processFiles: data => {
    let key = undefined;
    let file = data.result.files[0];
    $.get('/process/' + file.resource_id,
      {},
      function(response) {
        if(response.Status === 'Complete') {
          if(dominator.processedFiles < 2) {
            dominator.files.get(response.FileName.split('.')[0]).data = response
            dominator.processedFiles++;
          }
          if(dominator.processedFiles == 2) {
            //Send only file type and name of the table created.
            let data = {};
            dominator.files.forEach((k, v) => {
              data[k.type] = k.data.TableName;
            });
            dominator.processed(data);
          }
        }
    });
  },

  /**
   * Callback funtion that triggers when each file returns a succesful state
   * from sqlizer
   */
  processed: data => {
    console.log('FALTA BLOQUEAR PANTALLA');
    $.get('/postproc/',
      data,
      function(response) {
        console.log(response);
    });
    console.log('LIBERAR PANTALLA');
  },

  /**
   * Mutex function for file types
   */
  setType: (file_id, file_type) => {
    let count = 0;
    let sel = $('select#' + file_id);

    dominator.files.forEach((v, k) => {
      if(sel.val() != undefined || sel.val().length > 0) {
        // Check values in select to be different
        if(file_id !== k) {
          console.log('el otro');
          if(dominator.files.get(k).type === sel.val()) {
            $('button.start').attr('disabled', 'disabled');
            sel.val("");
            dominator.files.get(k).type = "";
            alert("File can't be of the same type");
            return;
          }
        }
        dominator.files.get(file_id).type = file_type;
        if(v.type != undefined && v.type.length != 0
          && dominator.queueFiles == 2) {
          count++;
          if(count == 2) {
          // Enable start upoad button only when files have file type
            dominator.queueReady = true;
            $('button.start').removeAttr('disabled');
          }
        }
      }

    });
  },

}

$(function () {
  'use strict';

  dominator.init($('#fileupload')); // Initialize the dominator

    // Initialize the jQuery File Upload widget:
    $('#fileupload').fileupload({
      uploadTemplate: dominator.uploadTemplate
    })
    .on('fileuploadadded', dominator.add)
    .on('fileuploadsend', function(e, data) {
      console.log(data.data.values());
     data.data = dominator.beforeUpload(data.data);
      console.log(data.data.values());
    })
    .on('fileuploadcompleted', function(e, data) {
      $('#fileupload_control .process').on('click',
        dominator.processFiles(data));
    });

    // Enable iframe cross-domain access via redirect option:
    $('#fileupload').fileupload(
        'option',
        'redirect',
        window.location.href.replace(
            /\/[^\/]*$/,
            '/cors/result.html?%s'
        )
    );
});
