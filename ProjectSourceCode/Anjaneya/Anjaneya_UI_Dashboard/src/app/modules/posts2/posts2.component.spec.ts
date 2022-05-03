import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts2Component } from './posts2.component';

describe('Posts2Component', () => {
  let component: Posts2Component;
  let fixture: ComponentFixture<Posts2Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts2Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts2Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
