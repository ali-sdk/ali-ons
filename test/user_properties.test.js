'use strict';

const mm = require('mm');
const assert = require('assert');
const httpclient = require('urllib');
const Consumer = require('../').Consumer;
const Producer = require('../').Producer;
const Message = require('../').Message;

const config = require(process.env.CONFIG || '../example/config');

const TOPIC = config.topic;

describe('test/user_properties.test.js', function() {
  this.timeout(20000);

  describe('#setUserProperties()', async function() {

    /**
     *  Test setUserProperties() functionality
     * @param {Object} userProperties the user properties to add to the message
     * @return {function(...[*]=)}
     */
    const testSetUserProperties = ({ userProperties }) =>
      function() {
        const message = new Message(null, null, null);
        message.setUserProperties(userProperties);
        Object.entries(userProperties)
          .forEach(([ k, v ]) => {
            assert.strictEqual(message.properties[k], v);
          });
      };

    it('should correctly set user properties', testSetUserProperties({ userProperties: { property1: 'value1', property2: 'value2' } }));
  });

  describe('#getUserProperties()', async function() {
    /**
     * Test getUserProperties() functionality
     * @param {Object} userProperties the user properties to add to the message
     * @returns {function(...[*]=)}
     */
    const testGetUserProperties = ({ userProperties }) =>
      function() {
        const message = new Message(null, null, null);
        message._userProperties = userProperties;
        const res = message.getUserProperties();
        Object.entries(userProperties)
          .forEach(([ k, v ]) => {
            assert.strictEqual(res[k], v);
          });
      };

    it('should correctly get user properties', testGetUserProperties({ userProperties: { property1: 'value1', property2: 'value2' } }));
  });


  describe('should send and receive messages with user-defined properties', async function() {
    let producer,
      consumer;

    before(async function() {
      producer = new Producer(Object.assign({
        httpclient,
      }, config));
      await producer.ready();
      consumer = new Consumer(Object.assign({
        httpclient,
      }, config));
      await consumer.ready();
    });

    after(async function() {
      await producer.close();
      await consumer.close();
    });

    afterEach(mm.restore);

    /**
     * Test get/setUserProperties() functionality
     * @param {string[]} tags the tag(s) optionally used to identify the message
     * @param {string} body the contents of the message payload
     * @param {Object} userProperties the user properties to add to the message
     * @return {function(...[*]=)}
     */
    const testFunctionality = ({ tags, body, userProperties }) =>
      async function() {
        const tagsString = tags ? tags.join('||') : '*';
        const msg = new Message(
          config.topic || TOPIC,
          tagsString,
          body
        );

        userProperties && msg.setUserProperties(userProperties);

        const sendResult = await producer.send(msg);
        assert(sendResult && sendResult.msgId);

        const msgId = sendResult.msgId;
        console.log(`Send Result: ${JSON.stringify(sendResult, null, 2)}`);

        await new Promise(r => {
          consumer.subscribe(config.topic || TOPIC, tagsString, async function(msg) {
            if (msg.msgId === msgId) {
              userProperties && Object.entries(userProperties)
                .forEach(([ k, v ]) => {
                  assert(msg.properties[k] === v);
                });
              body && assert(msg.body.toString() === body);
              r();
            }
          });
        });
      };

    it('correctly sends and receives message with user-defined properties', testFunctionality({ tags: null, body: 'Test body', userProperties: { prop1: 'val1', prop2: 'val2' } }));
  });
});

