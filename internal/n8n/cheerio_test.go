package n8n

import (
	"encoding/json"
	"strings"
	"testing"
)

// runCheerioCode runs code in a Code node (runOnceForAllItems) and returns
// the first output item's JSON, or the step error.
func runCheerioCode(t *testing.T, code string) (map[string]interface{}, string) {
	t.Helper()
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Code", "code", 2, map[string]interface{}{"jsCode": code})}, "Start->Code")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	if steps["Code"].Error != "" {
		return nil, steps["Code"].Error
	}
	out := decodeOutput(steps["Code"].Output)
	if len(out) == 0 || len(out[0]) == 0 {
		t.Fatalf("no output")
	}
	return out[0][0].JSON, ""
}

const cheerioPage = `<!DOCTYPE html><html><head><title>Shop</title></head><body>
<ul id="items">
  <li class="item hot" data-id="1" data-meta='{"a":1}'><a href="/a">Apple</a> <span class="price">1.50</span></li>
  <li class="item" data-id="2"><a href="/b">Banana</a> <span class="price">0.25</span></li>
  <li class="item sold" data-id="3"><a href="/c">Cherry</a></li>
</ul>
<form id="f"><input name="q" value="x y"><input type="checkbox" name="c" checked><select name="s"><option value="1">One</option><option value="2" selected>Two</option></select><textarea name="t">hi</textarea></form>
<p>a<!-- note -->b</p>
</body></html>`

func TestCodeCheerioQuery(t *testing.T) {
	code := "const html = " + mustJSON(cheerioPage) + `;
const cheerio = require('cheerio');
const $ = cheerio.load(html);
const items = $('#items li').map((i, el) => ({
  name: $(el).find('a').text(),
  href: $(el).find('a').attr('href'),
  price: $(el).find('.price').text() || null,
  id: $(el).data('id'),
})).get();
const first = $('li').first();
const out = {
  title: $('title').text(),
  count: $('li.item').length,
  items,
  hot: first.hasClass('hot'),
  meta: first.data('meta'),
  dataAll: first.data(),
  notSold: $('li').not('.sold').length,
  filterFn: $('li').filter((i, el) => $(el).find('.price').length > 0).length,
  is: $('li').eq(2).is('.sold'),
  last: $('li').last().find('a').text(),
  eqNeg: $('li').eq(-2).attr('data-id'),
  parent: $('a').first().parent().attr('data-id'),
  closest: $('.price').first().closest('ul').attr('id'),
  next: $('li').first().next().data('id'),
  prevAll: $('li').last().prevAll().length,
  siblings: $('li').first().siblings().length,
  children: $('#items').children().length,
  has: $('li').has('span').length,
  index: $('li.sold').index(),
  inner: $('li').eq(1).html(),
  outer: $.html($('a').first()),
  tag: first.prop('tagName'),
  type: first[0].type,
  name: first[0].name,
  attribs: first[0].attribs['data-id'],
  parentName: first[0].parent.name,
  childTypes: first[0].children.map(c => c.type).join(','),
  comment: $('p').contents().filter((i, n) => n.type === 'comment').get().map(n => n.data),
  ctx: $('a', '#items').length,
  val: [$('[name=q]').val(), $('[name=s]').val(), $('textarea').val()],
  serialize: $('#f').serialize(),
  each: (() => { let n = 0; $('li').each(function (i) { n += i; if (i === 1) return false; }); return n; })(),
  iter: [...$('a')].length,
  missing: $('nope').attr('x') === undefined && $('nope').html() === null,
  extract: $.extract({ links: [{ selector: 'a', value: 'href' }], title: 'title' }),
  rootText: $.root().find('title').text(),
  staticText: cheerio.text($('a')),
  fragment: cheerio.load('<b>x</b><i>y</i>', null, false).html(),
};
return [{ json: out }];`
	j, errMsg := runCheerioCode(t, code)
	if errMsg != "" {
		t.Fatal(errMsg)
	}
	want := map[string]interface{}{
		"title": "Shop", "count": 3.0, "hot": true, "notSold": 2.0, "filterFn": 2.0, "is": true, "last": "Cherry",
		"eqNeg": "2", "parent": "1", "closest": "items", "next": 2.0, "prevAll": 2.0, "siblings": 2.0, "children": 3.0,
		"has": 2.0, "index": 2.0, "inner": `<a href="/b">Banana</a> <span class="price">0.25</span>`, "outer": `<a href="/a">Apple</a>`,
		"tag": "LI", "type": "tag", "name": "li", "attribs": "1", "parentName": "ul", "childTypes": "tag,text,tag",
		"ctx": 3.0, "serialize": "q=x+y&c=on&s=2&t=hi", "each": 1.0, "iter": 3.0, "missing": true, "rootText": "Shop",
		"staticText": "AppleBananaCherry", "fragment": "<b>x</b><i>y</i>",
	}
	for k, v := range want {
		if j[k] != v {
			t.Errorf("%s = %#v, want %#v", k, j[k], v)
		}
	}
	got, _ := json.Marshal([]interface{}{j["items"], j["meta"], j["dataAll"], j["comment"], j["val"], j["extract"]})
	const wantJSON = `[[{"href":"/a","id":1,"name":"Apple","price":"1.50"},{"href":"/b","id":2,"name":"Banana","price":"0.25"},{"href":"/c","id":3,"name":"Cherry","price":null}],` +
		`{"a":1},{"id":1,"meta":{"a":1}},[" note "],["x y","2","hi"],{"links":["/a","/b","/c"],"title":"Shop"}]`
	if string(got) != wantJSON {
		t.Errorf("structured results:\n got %s\nwant %s", got, wantJSON)
	}
}

func TestCodeCheerioManipulation(t *testing.T) {
	code := `
const $ = require('cheerio').load('<div id="r"><p class="a">one</p><p>two</p><script>x()</script></div>', null, false);
$('script').remove();
$('p').addClass('b c').removeClass('a').attr('data-x', (i) => 'n' + i);
$('p').first().toggleClass('c').css('color', 'red');
$('#r').append('<span>end</span>').prepend($('<em>start</em>'));
$('p').last().after('<hr>').before('<br>');
$('em').replaceWith('<strong>S</strong>');
$('span').text('<done>');
$('p').eq(1).html('<i>2</i>');
$('#r').find('br').wrap('<section></section>');
const clone = $('strong').clone().attr('id', 'cl');
$('#r').append(clone);
$('p').removeAttr('data-x');
const moved = $('<ul><li>1</li><li>2</li></ul>');
moved.find('li').last().appendTo(moved.find('li').first());
return [{ json: { html: $.html(), moved: moved.toString(), css: $('p').first().css('color') } }];`
	j, errMsg := runCheerioCode(t, code)
	if errMsg != "" {
		t.Fatal(errMsg)
	}
	const want = `<div id="r"><strong>S</strong><p class="b" style="color: red;">one</p><section><br/></section><p class="b c"><i>2</i></p><hr/><span>&lt;done&gt;</span><strong id="cl">S</strong></div>`
	if j["html"] != want {
		t.Errorf("html:\n got %s\nwant %s", j["html"], want)
	}
	if j["moved"] != "<ul><li>1<li>2</li></li></ul>" || j["css"] != "red" {
		t.Errorf("moved=%v css=%v", j["moved"], j["css"])
	}
}

func TestCodeCheerioErrorsAndPerItem(t *testing.T) {
	j, errMsg := runCheerioCode(t, `
const c1 = require('cheerio'), c2 = require("cheerio");
const $ = c1.load(Buffer.from('<a href="u">t</a>'));
let caught = '';
try { $('p['); } catch (e) { caught = e.message; }
return [{ json: { same: c1 === c2 && c1.default === c1, href: $('a').attr('href'), caught } }];`)
	if errMsg != "" || j["same"] != true || j["href"] != "u" || !strings.Contains(j["caught"].(string), "invalid selector") {
		t.Fatalf("module: %v %s", j, errMsg)
	}

	// runOnceForEachItem: each item loads its own document in one runtime.
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Items", "code", 2, map[string]interface{}{"jsCode": "return [1,2,3].map(n => ({json: {html: '<b>' + n + '</b>'}}));"}),
		nd("Code", "code", 2, map[string]interface{}{"mode": "runOnceForEachItem", "jsCode": "const $ = require('cheerio').load($json.html); return {json: {v: $('b').text()}};"}),
	}, "Start->Items", "Items->Code")
	_, steps := h.run(t, data, "")
	out := decodeOutput(steps["Code"].Output)
	if steps["Code"].Error != "" || len(out) == 0 || len(out[0]) != 3 || out[0][2].JSON["v"] != "3" {
		t.Fatalf("per item: %v %s", out, steps["Code"].Error)
	}
}
