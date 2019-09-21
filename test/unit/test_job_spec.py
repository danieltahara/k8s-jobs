import yaml

from k8s_jobs.spec import YamlFileSpecSource

class TestSpecSource:
    def test_yaml_config_source_reloads(self, request, tmp_path):
        d1 = {"foo": "bar"}
        d2 = {"biz": "buzz"}
        tmp_file_name = tmp_path / request.node.name

        with open(tmp_file_name, "w+") as f:
            yaml.dump(d1, f)
        c = YamlFileSpecSource(str(tmp_file_name))
        assert d1 == c.get()

        with open(tmp_file_name, "w+") as f:
            yaml.dump(d2, f)
        assert d2 == c.get()

    def test_yaml_config_source_templates(self, request, tmp_path):
        jinja_d = {"biz": "{{ buzz }}"}
        tmp_file_name = tmp_path / request.node.name
        with open(tmp_file_name, "w+") as f:
            yaml.dump(jinja_d, f)

        c = YamlFileSpecSource(str(tmp_file_name))

        assert {"biz": "foo"} == c.get(template_args={"buzz": "foo"})
