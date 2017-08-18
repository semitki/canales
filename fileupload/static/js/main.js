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
    dominator.queueFiles++;
    if (dominator.queueFiles > 2) {
      // TODO Ugh! >.<'  very ugly hack to avoid adding more than two files
      data.context[0].remove();
    }
    if(dominator.queueFiles <= 2) {
      let element = $('select#' + data.files[0].name.split('.')[0]);
      dominator.files.set(element[0].id,
        {
          type: ''
        }
      );
      element.on('change', (e) => {
        dominator.setType(element[0].id, e.currentTarget.value);
      });
    }
  },


  /**
   * Callback that fires after uploads are done, upload status does not matter
   */
  afterUpload: () => {
    $('button.start').attr('disabled', 'disabled');
  },


  /**
   * Callback triggered when Process button is clicked and sets some additional
   * data to be sent in the request
   */
  beforeUpload: data => {
    $.blockUI({message: 'Uploading files'});
    if(dominator.queueFiles == 2) {
      let name = data.get('file').name.split('.')[0];
      let file_type = dominator.files.get(name).type;
      data.set('file_type', file_type);
      // TODO $('button.process').removeAttr('disabled');
    } else {
      alert('There must be at least 2 files to upload!');
    }
    return data;
  },


  /**
   * Callback function to send uploaded files to processing in sqlizer
   */
  processFiles: (data) => {
    $.blockUI({message:
      'Converting to SQL. Be patient it can take several minutes'});
    //data.preventDefault();
    let key = undefined;
    let file = data.result.files[0];
    $.get('/process/' + file.resource_id,
      {},
      function(response) {
        // TODO Weird, if I disable the process button outside the $.get request,
        // it is triggered automatically on diabling the button programatically
        // TODO $('button.process').attr('disabled', 'disabled');
        if(response.Status === 'Complete') {
          if(dominator.processedFiles < 2) {
            dominator.files.get(response.FileName.split('.')[0]).data = response
            dominator.processedFiles++;
          }
          if(dominator.processedFiles == 2) {
            $.blockUI({message: 'Generating report'});
            //Send only file type and name of the table created.
            let data = {};
            dominator.files.forEach((k, v) => {
              data[k.type] = k.data.TableName;
            });
            dominator.processed(data);
          }
        }
    })
    .fail(() => { $.unblockUI(); console.log(file); });
  },


  /**
   * Callback funtion that triggers when each file returns a succesful state
   * from sqlizer
   */
  processed: data => {
    $('button.delete').hide();
    $('input.toggle').hide();
    $.get('/postproc/',
      data,
      function(response) {
        $('#finale').append(
          '<a href="/' + response.reportLink + '">'
          + response.reportName + '</a>'
        );
        alert('Report generated succesfully');
        console.log(response);
    })
    .fail(() => {
      alert('Report failed');
    });
    $.unblockUI();
  },


  /**
   * Mutex function for file types
   */
  setType: (file_id, file_type) => {
    // TODO still does not work, various use cases fail
    // FIX IT ASAP!
    let count = 0;
    dominator.files.forEach((v, k) => {
      let sel = $('select#' + file_id);
      if(sel.val() != undefined || sel.val().length > 0) {
        dominator.files.get(file_id).type = file_type;
        // Check values in select to be different
        if(file_id !== k) {
          if(dominator.files.get(k).type === sel.val()) {
            $('button.start').attr('disabled', 'disabled');
            $('select#'+k).val("");
            dominator.files.get(k).type = "";
            alert("Files can't be of the same type");
            return;
          }
        }
        if(v.type != undefined && v.type.length != 0
          && dominator.queueFiles >= 2) {
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


// App starts here!
$(function () {
  'use strict';

  dominator.init($('#fileupload')); // Initialize the dominator

  // Initialize the jQuery File Upload widget:
  $('#fileupload').fileupload({
    acceptFileTypes: /(\.|\/)(csv)$/i
  })
  .on('fileuploadadded', dominator.add)
  .on('fileuploadcompleted', function(e, data) {
    $('#fileupload_control .process').on('click',
      dominator.processFiles(data));
  })
  .on('fileuploadsend', function(e, data) {
    data.data = dominator.beforeUpload(data.data);
  });
});
