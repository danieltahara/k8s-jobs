from unittest.mock import Mock
import yaml

from kubernetes.client import V1Job, V1ObjectMeta

from k8s_jobs.spec import JobGenerator, StaticJobSpecSource, YamlFileSpecSource


# TODO: Tests for configmap spec (mocked unit tests + itest)
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


class TestJobGenerator:
    def test_unique_names(self):
        generator = JobGenerator(
            StaticJobSpecSource(
                V1Job(metadata=V1ObjectMeta(name="iloveyouabushelandapeck"))
            )
        )

        j1 = generator.generate()
        j2 = generator.generate()

        assert (
            j1.metadata.name != j2.metadata.name
        ), "Each generated job must have a unique name"

    def test_generate_with_dict_config(self):
        job = V1Job(metadata=V1ObjectMeta(name="iloveyouabushelandapeck"))
        generator = JobGenerator(StaticJobSpecSource(job.to_dict()))

        j = generator.generate()
        assert (
            j["metadata"]["name"] != job.metadata.name
        ), "Should have mutated job name"

    def test_generate_with_template_args(self):
        mock_config_source = Mock()
        mock_config_source.get.return_value = V1Job(
            metadata=V1ObjectMeta(name="anotherone")
        )
        generator = JobGenerator(mock_config_source)
        template_args = {"foo": "bar"}

        generator.generate(template_args=template_args)

        mock_config_source.get.assert_called_once_with(template_args=template_args)

    def test_long_name(self):
        mock_config_source = Mock()
        mock_config_source.get.return_value = V1Job(
            metadata=V1ObjectMeta(
                name="thisisanextremelylongnamethathasalotofcharacters"
            )
        )
        generator = JobGenerator(mock_config_source)

        job = generator.generate()

        assert len(job.metadata.name) == 63

    def test_short_name(self):
        mock_config_source = Mock()
        mock_config_source.get.return_value = V1Job(
            metadata=V1ObjectMeta(name="shortname")
        ).to_dict()
        generator = JobGenerator(mock_config_source)

        job = generator.generate()

        assert job["metadata"]["name"].startswith("shortname-")
        assert len(job["metadata"]["name"]) == 9 + 1 + 2 * JobGenerator.SUFFIX_BYTES
