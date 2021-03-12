import itertools
import json
import zipfile

import luigi
import ujson


class SplitEvents(luigi.Task):
    input_zipfile = luigi.Parameter()
    output_dir = luigi.Parameter()

    def input(self):
        return luigi.LocalTarget(self.input_zipfile)

    def output(self):
        return luigi.LocalTarget(f"{self.output_dir}/match_ids.json")

    def run(self):
        files = []

        with zipfile.ZipFile(self.input().path, 'r') as zip_ref:
            for file in zip_ref.infolist():
                with zip_ref.open(file.filename, 'r') as fp:
                    data = ujson.load(fp)

                for match_id, events in itertools.groupby(data, key=lambda event: event['matchId']):
                    with luigi.LocalTarget(f"{self.output_dir}/{match_id}.json").open('w') as fp:
                        ujson.dump(list(events), fp)

                    files.append(f"{match_id}.json")

        with self.output().open('w') as fp:
            ujson.dump(files, fp)


class SplitMatches(luigi.Task):
    input_zipfile = luigi.Parameter()
    output_dir = luigi.Parameter()

    def input(self):
        return luigi.LocalTarget(self.input_zipfile)

    def output(self):
        return luigi.LocalTarget(f"{self.output_dir}/match_ids.json")

    def run(self):
        files = []

        with zipfile.ZipFile(self.input().path, 'r') as zip_ref:
            for file in zip_ref.infolist():
                with zip_ref.open(file.filename, 'r') as fp:
                    data = ujson.load(fp)

                for match in data:
                    match_id = match['wyId']
                    match['sourceFile'] = file.filename
                    with luigi.LocalTarget(f"{self.output_dir}/{match_id}.json").open('w') as fp:
                        ujson.dump(match, fp)

                    files.append(f"{match_id}.json")

        with self.output().open('w') as fp:
            ujson.dump(files, fp)


class WriteOutput(luigi.Task):
    teams_filename = luigi.Parameter()
    players_filename = luigi.Parameter()
    match_filename = luigi.Parameter()
    events_filename = luigi.Parameter()
    output_file = luigi.Parameter()

    def input(self):
        return dict(
            teams=luigi.LocalTarget(self.teams_filename),
            players=luigi.LocalTarget(self.players_filename),
            match=luigi.LocalTarget(self.match_filename),
            events=luigi.LocalTarget(self.events_filename)
        )

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        with self.input()['teams'].open('r') as fp:
            teams = {
                team['wyId']: team
                for team in ujson.load(fp)
            }

        with self.input()['players'].open('r') as fp:
            players = {
                player['wyId']: dict(
                    playerId=player['wyId'],
                    player=player
                )
                for player in ujson.load(fp)
            }

        with self.input()['match'].open('r') as fp:
            match = ujson.load(fp)

        with self.input()['events'].open('r') as fp:
            events = ujson.load(fp)

        home_team = away_team = None
        for team in match['teamsData'].values():
            if team['side'] == 'home':
                home_team = team
            else:
                away_team = team

        match_teams = {
            home_team['teamId']: teams[home_team['teamId']],
            away_team['teamId']: teams[away_team['teamId']]
        }

        match_players = {
            home_team['teamId']: [
                players.get(player['playerId'])
                for player in (
                        home_team['formation']['lineup'] +
                        home_team['formation']['bench']
                )
            ],
            away_team['teamId']: [
                players.get(player['playerId'])
                for player in (
                        away_team['formation']['lineup'] +
                        away_team['formation']['bench']
                )
            ]
        }

        output = {
            'events': events,
            'teams': match_teams,
            'players': match_players
        }

        # TODO: Does ujson also keep order?
        with self.output().open('w') as fp:
            json.dump(output, fp)


class WriteIndex(luigi.Task):
    matches_filename = luigi.Parameter()
    matches_input_dir = luigi.Parameter()
    output_filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.output_filename)

    def input(self):
        return luigi.LocalTarget(self.matches_filename)

    def run(self):
        with self.input().open('r') as fp:
            match_files = sorted(ujson.load(fp))

        output_data = """
| ID | Label | Date | Source file |
|----|-------|------|-------------|
"""

        for filename in match_files:
            with luigi.LocalTarget(f"{self.matches_input_dir}/{filename}").open('r') as fp:
                match_data = ujson.load(fp)

                output_data += f"|[{match_data['wyId']}](files/{match_data['wyId']}.json)|{match_data['label']}|{match_data['date']}|{match_data['sourceFile']}|\n"

        with self.output().open('w') as fp:
            fp.write(output_data)


class KloppyProcessor(luigi.Task):
    input_dir = luigi.Parameter()
    tmp_dir = luigi.Parameter()
    output_dir = luigi.Parameter()

    def requires(self):
        return dict(
            events=SplitEvents(
                input_zipfile=f"{self.input_dir}/events.zip",
                output_dir=f"{self.tmp_dir}/events"
            ),
            matches=SplitMatches(
                input_zipfile=f"{self.input_dir}/matches.zip",
                output_dir=f"{self.tmp_dir}/matches"
            )
        )

    def run(self):
        with self.input()['events'].open('r') as fp:
            event_files = set(ujson.load(fp))

        with self.input()['matches'].open('r') as fp:
            match_files = set(ujson.load(fp))

        complete = event_files.symmetric_difference(match_files)
        assert not complete, "Missing some data"

        for filename in match_files:
            yield WriteOutput(
                teams_filename=f"{self.input_dir}/teams.json",
                players_filename=f"{self.input_dir}/players.json",
                match_filename=f"{self.tmp_dir}/matches/{filename}",
                events_filename=f"{self.tmp_dir}/events/{filename}",
                output_file=f"{self.output_dir}/files/{filename}"
            )

        yield WriteIndex(
            matches_filename=self.input()['matches'].path,
            matches_input_dir=f"{self.tmp_dir}/matches",
            output_filename=f"{self.output_dir}/README.md"
        )


if __name__ == "__main__":
    task = KloppyProcessor(
        input_dir="../raw_data",
        tmp_dir="../tmp",
        output_dir="../processed"
    )

    luigi.build([task], local_scheduler=True)